module config;

import std.stdio;
import std.file;
import std.string;
import std.conv;
import std.algorithm;
import std.array;
import std.traits;

static struct Config
{
    int scan_max_depth;
    int max_files_per_chunk;
    int min_bands_needed;
    string[] allowed_extensions;
    string[] bands;

    size_t max_memory_mb;
    int max_retries;
    int tick_interval_ms;

    string toString() const
    {
        import std.format : format;
        return format(
            "CONFIG INFORMATION\n\tscan_max_depth=%s, max_files_per_chunk=%s, min_bands_needed=%s, 
\tallowed_extensions=%s, bands=%s, \n\tmax_memory_mb=%s, max_retries=%s, tick_interval_ms=%s",
            scan_max_depth,
            max_files_per_chunk,
            min_bands_needed,
            allowed_extensions,
            bands,
            max_memory_mb,
            max_retries,
            tick_interval_ms
        );
    }
}

public Config loadConfig(string filePath)
{
    Config cfg;

    // Set some "Safety Defaults" in case the file is missing keys
    cfg.scan_max_depth = 3;
    cfg.max_files_per_chunk = 40;
    cfg.allowed_extensions = [".TIF", ".TIFF", ".NPY", ".TXT"];
    cfg.bands = ["R", "G", "NIR", "RE"];
    cfg.min_bands_needed = cast(int)cfg.bands.length;
    cfg.max_memory_mb = 4096; // 4GB default
    cfg.max_retries = 3;
    cfg.tick_interval_ms = 100;

    if (!filePath.exists)
    {
        return cfg;
    }

    foreach (line; File(filePath).byLine)
    {
        if (line.strip.length == 0 || line.strip.startsWith("#"))
            continue;
        
        auto parts = line.split("=");
        if (parts.length < 2)
            continue;

        string key = parts[0].strip().idup;
        string val = parts[1].strip().idup;

        // Automatically map keys to struct members
        switch (key)
        {
            case "SCAN_MAX_DEPTH":
                cfg.scan_max_depth = val.to!int;
                break;
            case "ALLOWED_EXTENSIONS":
                cfg.allowed_extensions = val.split(",").map!(s => s.strip.idup).array;
                break;
            case "MAX_FILES_PER_CHUNK":
                cfg.max_files_per_chunk = val.to!int;
                break;
            case "BANDS":
                cfg.bands = val.split(",").map!(s => s.strip.idup).array;
                cfg.min_bands_needed = cast(int)cfg.bands.length;
                break;
            case "MAX_MEMORY_MB":
                cfg.max_memory_mb = val.to!size_t;
                break;
            case "MAX_RETRIES":
                cfg.max_retries = val.to!int;
                break;
            case "TICK_INTERVAL_MS":
                cfg.tick_interval_ms = val.to!int;
                break;
            default:
                break;
        }
    }
    return cfg;
}
