module planning;

import std.stdio;
import std.file;
import std.path;
import std.algorithm;
import std.range;
import std.array;
import std.string;
import std.logger;
import std.datetime.systime : SysTime, Clock;
import std.conv : to;
import std.json;

import omspec_ipc;
import config;

private int MAX_FILES_PER_CHUNK;
private string[] ALLOWED_EXTENSIONS;
private int MIN_BANDS_NEEDED;
private string[] BANDS_ALLOWED;

// Config setup
private Config cfg;

SysTime current_time;
string log_filename;
FileLogger fileLogger;

static this()
{
    if (!exists("log"))
        mkdir("log");
    current_time = Clock.currTime();
    log_filename = "log\\planning_" ~ current_time.toISOExtString().replace(":", "-") ~ ".log";
    fileLogger = new FileLogger(log_filename, LogLevel.info, CreateFolder.yes);
}

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

        fileLogger.infof("Scanning directory: %s", ds.path);
        foreach (entry; entries)
        {
            if (!entry.isFile)
                continue;

            // std.path functions
            string extn = extension(entry.name);
            string base = baseName(entry.name, extn); // Get filename without path or extension

            if (!ALLOWED_EXTENSIONS.canFind(extn.toUpper()))
            {
                fileLogger.warningf("extension %s not allowed", extn);
                continue;
            }

            auto img_base_idx = base.lastIndexOf('_');

            // Check if underscore exists AND isn't at the very start/end
            if (img_base_idx <= 0 || img_base_idx >= (to!int(base.length) - 1))
            {
                fileLogger.warningf("File %s skipped: naming convention mismatch", base);
                continue;
            }

            string img_base = base[0 .. img_base_idx];
            string img_band = base[img_base_idx + 1 .. $].toUpper();

            if (!BANDS_ALLOWED.canFind(img_band))
            {
                fileLogger.warningf("Band %s in file %s not recognized", img_band, base);
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
            found_multispectral_images[img_base].file_size += getSize(entry.name);
        }
    }

    DatasetChunk[] chunks = get_chunks(found_multispectral_images, MAX_FILES_PER_CHUNK);
    total_plan ~= chunks;

    return total_plan;
}

public void save_plan_to_json(DatasetChunk[] chunks, string output_path)
{
    JSONValue[] json_chunks;

    foreach (size_t i, chunk; chunks)
    {
        JSONValue j_chunk;
        j_chunk["chunk_id"] = i;
        j_chunk["image_count"] = chunk.images.length;
        j_chunk["chunk_size"] = chunk.chunk_size;

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

            j_images ~= j_img;
        }

        j_chunk["images"] = j_images;
        json_chunks ~= j_chunk;
    }

    JSONValue final_root = JSONValue(json_chunks);

    // Write to file with pretty printing for debugging
    std.file.write(output_path, final_root.toPrettyString());

    fileLogger.info("Plan successfully serialized to: " ~ output_path);
}

// Recursive discovery of folders containing files
private void scan_directory_recursive(string currentPath, int currentDepth, int maxDepth, ref Dataset[] datasets)
{
    if (currentDepth > maxDepth)
        return;
    if (!exists(currentPath) || !isDir(currentPath))
        return;

    // Use a lazy range to find if ANY file matches our criteria
    auto validFiles = dirEntries(currentPath, SpanMode.shallow)
        .filter!(e => e.isFile &&
                ALLOWED_EXTENSIONS.canFind(extension(e.name).toUpper()));

    // Use walkLength or a loop to get the count without loading the files into memory
    long validCount = validFiles.walkLength;

    if (validCount > 0)
    {
        datasets ~= Dataset(currentPath, validCount);
        fileLogger.infof("Valid dataset found: %s (%d candidate files)", currentPath, validCount);
    }

    auto subDirs = dirEntries(currentPath, SpanMode.shallow).filter!(e => e.isDir);
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

    foreach (img_base; images.keys)
    {
        auto img_group = images[img_base];

        // Reject image groups with less than required bands
        if (img_group.bands.length < MIN_BANDS_NEEDED)
        {
            fileLogger.warningf("Image group %s skipped: only %d bands found", img_base, img_group
                    .bands.length);
            continue;
        }

        if (current_chunk.file_count + 1 > MAX_CHUNK_SIZE)
        {
            chunks ~= current_chunk;
            current_chunk = DatasetChunk();
            current_chunk.file_count = 0;
            current_chunk.chunk_size = 0;
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
