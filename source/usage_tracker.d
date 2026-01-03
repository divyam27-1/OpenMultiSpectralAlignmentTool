module usage_tracker;

import std.file;
import std.json;
import std.datetime;
import std.process : environment;
import std.algorithm;
import std.range;
import std.path;
import std.stdio : writeln, writefln;

struct UsageStats
{
    string lastDate;
    ulong bytesProcessed;
}

class UsageTracker
{
    private string cfgDataPath;
    private const ulong limit = 1024UL * 1024 * 1024 * 1024; // 1 TB

    this()
    {
        // Fallback to current path if LOCALAPPDATA is missing (rare on Windows)
        string appData = environment.get("LOCALAPPDATA", ".");
        string dir = buildPath(appData, "OMSPEC");

        try
        {
            if (!exists(dir))
                mkdirRecurse(dir);
            cfgDataPath = buildPath(dir, "cfgdata.dat");
        }
        catch (Exception e)
        {
            // If we can't create the folder, use the current directory
            cfgDataPath = "cfgdata.dat";
        }
    }

    bool incrementAndCheck(ulong additionalBytes)
    {
        auto stats = loadStats();

        version (release)
        {
        }
        else
        {
            // Dev Build: Always bypass and return true
            writeln("DEBUG: UsageTracker bypass enabled (Dev Build).");
            return true;
        }

        string today = Clock.currTime().toISOString()[0 .. 8];

        // New day reset
        if (stats.lastDate != today)
        {
            stats.lastDate = today;
            stats.bytesProcessed = 0;
        }

        // Check if we are ALREADY over the limit from previous runs
        if (stats.bytesProcessed > limit || additionalBytes > limit)
        {
            writeln("--------------------------------------------------");
            writeln("ERROR: Daily limit of 1TB exceeded (Community Edition).");
            writeln("Please wait until tomorrow or upgrade to Pro.");
            writeln(
                "If you are a researcher, please email me at divyam2701@gmail.com for a free activation key");
            writeln("--------------------------------------------------");
            return false;
        }

        // Log the increment (happens after the check, so the current run is "grandfathered in")
        stats.bytesProcessed += additionalBytes;
        saveStats(stats);
        return true;
    }

    UsageStats loadStats()
    {
        if (!exists(cfgDataPath))
        {
            return UsageStats(Clock.currTime().toISOString()[0 .. 8], 0);
        }

        try
        {
            ubyte[] data = cast(ubyte[]) read(cfgDataPath);
            // Flip the bits back to get the original JSON string
            ubyte[] j_bytes = data.map!(a => cast(ubyte)~a).array();
            string j_str = cast(string) j_bytes;

            JSONValue j = parseJSON(j_str);
            return UsageStats(j["lastDate"].str, j["bytesProcessed"].get!ulong);
        }
        catch (Exception e)
        {
            // CORRUPTION HANDLING: If file is tampered with or malformed,
            // we return a fresh stats object instead of crashing the app.
            return UsageStats(Clock.currTime().toISOString()[0 .. 8], 0);
        }
    }

    void saveStats(UsageStats stats)
    {
        try
        {
            JSONValue j = JSONValue(["lastDate": stats.lastDate]); // Initialize with string
            j.object["bytesProcessed"] = JSONValue(stats.bytesProcessed); // Add the ulong

            string j_str = j.toString();

            // Obfuscation: Convert to array to force evaluation of the lazy map
            ubyte[] j_bytes_inv = (cast(ubyte[]) j_str).map!(a => cast(ubyte)~a).array();

            std.file.write(cfgDataPath, j_bytes_inv);
        }
        catch (Exception e)
        {
            writeln("Warning: Could not save usage stats: ", e.msg);
        }
    }
}
