module planning;

import std.stdio;
import std.file;
import std.path;
import std.algorithm;
import std.range;
import std.array;
import std.string;
import std.logger;
import std.datetime.systime : Clock;
import std.conv : to;
import std.json;

import planning_h;
import appstate;
import config;
import loggers : planLogger, workerLogPath;

private int MAX_FILES_PER_CHUNK;
private string[] ALLOWED_EXTENSIONS;
private int MIN_BANDS_NEEDED;
private string[] BANDS_ALLOWED;

// Config setup
private Config cfg;

/** * Scans the target and builds the Chunk array.
 */
public DatasetChunk[] generate_plan(string root_path, int maxDepth)
{
    cfg = loadConfig(buildPath(thisExePath().dirName, "omspec.cfg").absolutePath());
    MAX_FILES_PER_CHUNK = cfg.max_files_per_chunk;
    ALLOWED_EXTENSIONS = cfg.allowed_extensions;
    MIN_BANDS_NEEDED = cfg.min_bands_needed;
    BANDS_ALLOWED = cfg.bands;

    DatasetChunk[] total_plan;
    Dataset[] found_dirs;

    scan_directory_recursive(root_path, 0, maxDepth, found_dirs);

    bool[string] found_bases;
    MultiSpectralImageGroup[string] found_multispectral_images;

    foreach (ds; found_dirs)
    {
        auto entries = dirEntries(ds.path, SpanMode.shallow);

        planLogger.infof("Scanning directory: %s", ds.path);
        foreach (entry; entries)
        {
            if (!entry.isFile)
                continue;

            // std.path functions
            string extn = extension(entry.name);
            string base = baseName(entry.name, extn); // Get filename without path or extension

            if (!ALLOWED_EXTENSIONS.canFind(extn.toUpper()))
            {
                planLogger.warningf("extension %s not allowed", extn);
                continue;
            }

            auto img_base_idx = base.lastIndexOf('_');

            // Check if underscore exists AND isn't at the very start/end
            if (img_base_idx <= 0 || img_base_idx >= (to!int(base.length) - 1))
            {
                planLogger.warningf("File %s skipped: naming convention mismatch", base);
                continue;
            }

            string img_base = base[0 .. img_base_idx];
            string img_band = base[img_base_idx + 1 .. $].toUpper();

            if (!BANDS_ALLOWED.canFind(img_band))
            {
                planLogger.warningf("Band %s in file %s not recognized", img_band, base);
                continue;
            }

            // Check 'Set' existence using 'in' operator
            if (img_base !in found_bases)
            {
                found_bases[img_base] = true;

                // Fixed: No named arguments in D. Initialize then set.
                MultiSpectralImageGroup group;
                found_multispectral_images[img_base] = group;
            }

            found_multispectral_images[img_base].bands ~= img_band;
            found_multispectral_images[img_base].fname[img_band] = entry.name;
            found_multispectral_images[img_base].file_size += getSize(entry.name); // TODO remove this after testing
        }
    }

    DatasetChunk[] chunks = get_chunks(found_multispectral_images, MAX_FILES_PER_CHUNK);
    total_plan ~= chunks;

    return total_plan;
}

public void save_plan_to_json(DatasetChunk[] chunks, string output_path)
{
    // since this can take some time we can implement progress bar for UX
    import tui_h;

    ProgressBar progBar = ProgressBar(to!int(chunks.length) + 1);

    JSONValue[] json_chunks;

    foreach (size_t i, chunk; chunks)
    {
        JSONValue j_chunk;
        j_chunk["chunk_id"] = i;
        j_chunk["image_count"] = chunk.images.length;
        j_chunk["chunk_size"] = chunk.chunk_size;
        j_chunk["logfile"] = workerLogPath.format(i);

        j_chunk["image_metadata"] = get_image_metadata(chunk.images[0]); // TODO for v2.0.0: make this robust if we have multiple different sensor sources in one chunk so one DJI camera image in one chunk and one Sony multispec camera image in same chunk this would not handle it currently

        JSONValue[] j_images;
        foreach (img; chunk.images)
        {
            JSONValue j_img;

            string sample_path = img.fname.values[0];
            string b_name = baseName(sample_path).stripExtension();

            auto idx = b_name.lastIndexOf('_');
            j_img["base_name"] = (idx != -1) ? b_name[0 .. idx] : b_name;
            j_img["bands"] = JSONValue(img.fname);
            j_img["size"] = img.file_size;
            j_img["extension"] = img.fname[img.bands[0]].extension;

            j_images ~= j_img;
        }

        j_chunk["images"] = j_images;
        json_chunks ~= j_chunk;

        progBar.update(to!int(i) + 1, 1);
    }

    JSONValue final_root = JSONValue(json_chunks);

    // Write to file with pretty printing for debugging
    std.file.write(output_path, final_root.toPrettyString());
    progBar.update(to!int(chunks.length) + 1, 0);

    planLogger.info("Plan successfully serialized to: " ~ output_path);
    progBar.finish();
}

private void scan_directory_recursive(string currentPath, int currentDepth, int maxDepth, ref Dataset[] datasets)
{
    if (currentDepth > maxDepth)
        return;
    if (!exists(currentPath) || !isDir(currentPath))
        return;

    // Skip if this directory itself is named "aligned"
    import std.path : baseName;
    import process_control_h : TaskMode, mode;

    if (mode == TaskMode.ALIGN && currentPath.baseName.toLower == "aligned")
    {
        planLogger.infof("Skipping output directory: %s", currentPath);
        return;
    }

    // Use a lazy range to find if ANY file matches our criteria
    auto validFiles = dirEntries(currentPath, SpanMode.shallow)
        .filter!(e => e.isFile &&
                ALLOWED_EXTENSIONS.canFind(extension(e.name).toUpper()));

    long validCount = validFiles.walkLength;

    if (validCount > 0)
    {
        datasets ~= Dataset(currentPath, validCount);
        planLogger.infof("Valid dataset found: %s (%d candidate files)", currentPath, validCount);
    }

    auto subDirs = dirEntries(currentPath, SpanMode.shallow)
        .filter!(e => e.isDir);

    foreach (dir; subDirs)
    {
        scan_directory_recursive(dir.name, currentDepth + 1, maxDepth, datasets);
    }
}

private DatasetChunk[] get_chunks(MultiSpectralImageGroup[string] images, int MAX_CHUNK_SIZE)
{
    DatasetChunk[] chunks;
    DatasetChunk current_chunk;
    current_chunk.file_count = 0;
    current_chunk.chunk_size = 0;

    int cid = 0;
    current_chunk.chunk_id = cid;

    foreach (img_base; images.keys)
    {
        auto img_group = images[img_base];

        // Reject image groups with less than required bands
        if (img_group.bands.length < MIN_BANDS_NEEDED)
        {
            planLogger.warningf("Image group %s skipped: only %d bands found", img_base, img_group
                    .bands.length);
            continue;
        }

        if (current_chunk.file_count + 1 > MAX_CHUNK_SIZE)
        {
            chunks ~= current_chunk;
            current_chunk = DatasetChunk();
            current_chunk.file_count = 0;
            current_chunk.chunk_size = 0;
            current_chunk.chunk_id = ++cid;
        }

        current_chunk.images ~= img_group;
        current_chunk.file_count++;
        current_chunk.chunk_size += img_group.file_size;
    }

    if (current_chunk.file_count > 0)
    {
        chunks ~= current_chunk;
    }

    return chunks;
}

private JSONValue get_image_metadata(MultiSpectralImageGroup image)
{
    import std.process : execute;

    string[string] bandPaths = image.fname;

    // Locate exiftool relative to the D binary
    string exifPath = buildPath(dirName(thisExePath()), "exiftool-13.45_64", "exiftool.exe");

    string[] requiredMetadata = [
        "-DewarpData",
        "-RelativeOpticalCenterX",
        "-RelativeOpticalCenterY",
        "-CalibratedOpticalCenterX",
        "-CalibratedOpticalCenterY",
        "-VignettingData",
        "-BlackLevel"
    ];

    JSONValue metadata = JSONValue(string[string].init);

    foreach (string band, string bandPath; bandPaths)
    {
        auto res = execute([exifPath, "-j"] ~ requiredMetadata ~ [bandPath]);

        if (res.status != 0)
        {
            planLogger.error(
                "Exiftool failed to extract metadata from band " ~ band ~ " at: " ~ bandPath);
            throw new Exception(
                "Exiftool failed to extract metadata from band " ~ band ~ " at: " ~ bandPath);
        }

        auto bandExifData = parseJSON(res.output);

        if (bandExifData.type != JSONType.array || bandExifData.array.length == 0)
        {
            planLogger.error("Exiftool returned invalid JSON for band: " ~ band);
            throw new Exception("Exiftool returned invalid JSON for band: " ~ band);
        }

        metadata[band] = bandExifData.array[0];

        if (
            !("DewarpData" in metadata[band]) ||
            !("VignettingData" in metadata[band]) ||
            metadata[band]["DewarpData"].type != JSONType.string ||
            metadata[band]["VignettingData"].type != JSONType.string
            )
        {
            planLogger.info(
                "Skipping Dewarp/Vignetting processing for band " ~ band ~
                    " (missing or invalid metadata)"
            );
            continue;
        }

        // format DewarpData into an array
        string dewarpData = metadata[band]["DewarpData"].str;
        long dewarpDataSemicon_idx = dewarpData.lastIndexOf(';');
        if (dewarpDataSemicon_idx == -1 || dewarpDataSemicon_idx >= (dewarpData.length - 1))
        {
            planLogger.error(
                "Exiftool did not return correctly formatted DewarpData for band: " ~ band);
        }

        dewarpData = dewarpData[dewarpDataSemicon_idx + 1 .. $];
        JSONValue dewarpArrayJSON = dewarpData.split(',')
            .map!(a => a.to!float)
            .array
            .JSONValue;

        metadata[band]["DewarpData"] = dewarpArrayJSON;

        import std.uni : isWhite;

        // format VignettingData into an array
        string vignetteData = metadata[band]["VignettingData"].str;
        JSONValue vignetteDataArrayJSON = vignetteData.filter!(c => !c.isWhite)
            .array
            .split(',')
            .map!(a => a.to!float)
            .array
            .JSONValue;

        metadata[band]["VignettingData"] = vignetteDataArrayJSON;
    }

    return metadata;
}
