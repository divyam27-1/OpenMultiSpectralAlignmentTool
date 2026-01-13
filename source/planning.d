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

    ProgressBar progBar = ProgressBar(to!int(chunks.length) + 2); // 1 extra step for parsing exifdata, 1 extra for writing to output file

    JSONValue[] json_chunks;

    foreach (size_t i, chunk; chunks)
    {
        JSONValue j_chunk;
        j_chunk["chunk_id"] = i;
        j_chunk["image_count"] = chunk.images.length;
        j_chunk["chunk_size"] = chunk.chunk_size;
        j_chunk["logfile"] = workerLogPath.format(i);

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

    import std.algorithm : map, joiner;

    MultiSpectralImageGroup[] exifImageBatch = chunks.map!(c => c.images[0]).array;
    JSONValue[] exifMetadata = get_image_batch_metadata(exifImageBatch);

    for (size_t i = 0; i < json_chunks.length; i++)
        json_chunks[i]["image_metadata"] = exifMetadata[i];

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
    import appstate : TaskMode, mode;

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

private JSONValue[] get_image_batch_metadata(MultiSpectralImageGroup[] batch)
{
    import std.file : write, remove;
    import std.path : buildPath;
    import std.process : execute;
    import std.json : parseJSON, JSONValue, JSONType;
    import appstate : targetPath;

    if (batch.length == 0)
        return JSONValue[].init;

    string exifPath = buildPath(dirName(thisExePath()), "exiftool-13.45_64", "exiftool.exe");
    if (!exists(exifPath))
    {
        planLogger.error("Exiftool not found at expected path: " ~ exifPath);
        throw new Exception("Exiftool not found at expected path: " ~ exifPath);
    }

    planLogger.infof("Using Exiftool at path: %s", exifPath);

    string[] flatPathList;
    foreach (image; batch)
        foreach (band; image.bands)
            flatPathList ~= image.fname[band];

    string exifPathsFile = buildPath(targetPath, "exif_paths.txt");
    std.file.write(exifPathsFile, flatPathList.join("\n"));

    version (release)
        scope (exit)
            if (exists(exifPathsFile))
                remove(exifPathsFile);

    string[] args = [
        exifPath,
        "-j", // JSON output
        "-q", "-q", // double quiet mode to supress all info and warnings
        "-DewarpData", "-RelativeOpticalCenterX", "-RelativeOpticalCenterY",
        "-CalibratedOpticalCenterX", "-CalibratedOpticalCenterY",
        "-VignettingData", "-BlackLevel",
        "-@",
        exifPathsFile
    ];

    planLogger.info("Starting batch metadata extraction via Exiftool...");

    auto res = execute(args);
    if (res.status != 0)
    {
        writeln("Exiftool Error Output");
        planLogger.error("Exiftool batch failed to extract metadata.");
        throw new Exception("Exiftool batch failed");
    }

    JSONValue[] allResults;
    try
    {
        allResults = parseJSON(res.output).array;
        if (allResults.length != flatPathList.length)
        {
            planLogger.error("Mismatch in Exiftool results count and input file count.");
            throw new Exception("Mismatch in Exiftool results count and input file count.");
        }
    }

    catch (JSONException e)
    {
        // TRAP: Save the exact output that caused the crash
        import std.file : write;

        std.file.write("CRASH_DUMP.json", res.output);

        planLogger.errorf("JSON Error: %s. Dumped raw output to CRASH_DUMP.json for inspection.", e
                .msg);
        throw e;
    }

    size_t resultIdx = 0;
    JSONValue[] finalMetadata;

    foreach (ref image; batch)
    {
        JSONValue groupContainer = JSONValue(string[string].init);

        foreach (band; image.bands)
        {
            JSONValue rawBandData = allResults[resultIdx++];

            rawBandData["DewarpData"] = get_dewarp_data_from_exif(rawBandData, band);
            rawBandData["VignettingData"] = get_vignetting_data_from_exif(rawBandData, band);

            groupContainer[band] = rawBandData;
        }

        finalMetadata ~= groupContainer;
    }

    planLogger.info("Successfully extracted metadata for all images.");
    return finalMetadata;
}

private JSONValue get_dewarp_data_from_exif(JSONValue rawData, string bandName)
{
    if ("DewarpData" !in rawData || rawData["DewarpData"].type != JSONType.string)
        return JSONValue(null);

    string data = rawData["DewarpData"].str;
    long semiIdx = data.lastIndexOf(';');

    // Slice after semicolon if it exists
    string cleanData = (semiIdx != -1) ? data[semiIdx + 1 .. $] : data;

    try
    {
        return cleanData.split(',')
            .map!(a => a.strip.to!float)
            .array
            .JSONValue;
    }
    catch (Exception e)
    {
        planLogger.error("Malformed DewarpData in band " ~ bandName);
        return JSONValue(null);
    }
}

private JSONValue get_vignetting_data_from_exif(JSONValue rawData, string bandName)
{
    if ("VignettingData" !in rawData || rawData["VignettingData"].type != JSONType.string)
        return JSONValue(null);

    import std.uni : isWhite;

    string data = rawData["VignettingData"].str;

    try
    {
        return data.filter!(c => !c.isWhite)
            .array
            .split(',')
            .map!(a => a.to!float)
            .array
            .JSONValue;
    }
    catch (Exception e)
    {
        planLogger.error("Malformed VignettingData in band " ~ bandName);
        return JSONValue(null);
    }
}