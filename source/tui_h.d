module tui_h;

import std.stdio;
import std.range : repeat;
import std.array : join;
import std.algorithm : min, max;

public struct ProgressBar
{
    private int totalCount;
    private int completedCount;
    private int totalRunning;
    private int failedCount;
    private int barWidth;

    this(int total, int width = 40)
    {
        this.totalCount = total;
        this.barWidth = width;
        this.completedCount = 0;
        this.failedCount = 0;
    }

    public void update(int completed, int running, int failed = 0)
    {
        this.completedCount = min(completed, totalCount);
        this.failedCount = min(failed, totalCount - completedCount);
        this.totalRunning = running;
        render();
    }

    private void render()
    {
        double percentSucess = (totalCount > 0) ? (cast(double) completedCount / totalCount) : 0;
        double percentFailed = (totalCount > 0) ? (cast(double) failedCount / totalCount) : 0;
        int filledWidth = cast(int)(percentSucess * barWidth);
        int failedWidth = cast(int)(percentFailed * barWidth);
        int emptyWidth = barWidth - filledWidth - failedWidth;

        // [==========>----------] 50% (5/10)   [3 Workers Running]
        string bar = "[" ~
            "=".repeat(max(0, filledWidth - 1))
            .join() ~
            (filledWidth > 0 ? ">" : "") ~
            "-".repeat(emptyWidth)
            .join() ~
            "X".repeat(failedWidth).join() ~
            "]";

        // \r moves cursor to start of line. 
        // writef keeps us on the same line.
        writef("\r%s %3.0f%% (%d/%d)\t[%d Workers Running]",
            bar, percentSucess * 100, completedCount, totalCount, totalRunning);
        stdout.flush(); // Force Windows/Linux to show the change immediately
    }

    public void finish()
    {
        writeln(); // Move to a new line when the job is done
        writeln("Finished");
    }
}
