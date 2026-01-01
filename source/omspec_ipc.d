import std.format;

public struct Dataset
{
	string path;
	long file_count;
}

public struct MultiSpectralImageGroup
{
	long file_size;
	string[] bands;
	string[string] fname;
}

public struct DatasetChunk
{
	size_t chunk_size;
	size_t file_count;
	MultiSpectralImageGroup[] images;
}
