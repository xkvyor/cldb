#ifndef _FILE_H_
#define _FILE_H_

#include "common.h"

NAMESPACE_BEGIN

class File {
private:
	char* filename_;
	FILE* fp_;

public:
	File(const char* filename);
	~File();

	bool exist();
	bool create();
	bool open();
	bool close();
	bool remove();

	size_t read(offset_t off, void* buf, size_t size);
	size_t write(offset_t off, const void* buf, size_t size);

	const char* getFilename() { return filename_; }
};

NAMESPACE_END

#endif