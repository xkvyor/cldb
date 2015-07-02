#include <unistd.h>
#include "file.h"

NAMESPACE_BEGIN

File::File(const char* filename): filename_(NULL), fp_(NULL)
{
	filename_ = strdup(filename);
}

File::~File()
{
	if (fp_)
	{
		close();
	}
	if (filename_)
	{
		free(filename_);
	}
}

bool File::exist()
{
	return (access(filename_, F_OK) != -1);
}

bool File::create()
{
	fp_ = fopen(filename_, "w+b");
	return (fp_ != NULL);
}

bool File::open()
{
	fp_ = fopen(filename_, "r+b");
	return (fp_ != NULL);
}

bool File::close()
{
	int ret = 0;
	if (fp_)
	{
		ret = fclose(fp_);
		fp_ = NULL;
	}
	return (ret == 0);
}

bool File::remove()
{
	return (::remove(filename_) == 0);
}

size_t File::read(offset_t off, void* buf, size_t size)
{
	fseek(fp_, off, SEEK_SET);
	return fread(buf, 1, size, fp_);
}

size_t File::write(offset_t off, const void* buf, size_t size)
{
	fseek(fp_, off, SEEK_SET);
	return fwrite(buf, 1, size, fp_);
}

NAMESPACE_END