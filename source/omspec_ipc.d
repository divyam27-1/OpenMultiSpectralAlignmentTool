import std.format;

public struct Dataset {
    string path;
    long file_count;
}

public struct MultiSpectralImageGroup {
	string directory;
	string[] bands;
	string[string] fname;
}

public struct DatasetChunk {
	MultiSpectralImageGroup[] images;
	long file_count;
}