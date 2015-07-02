#ifndef _MEMPOOL_H_
#define _MEMPOOL_H_

#include "common.h"

NAMESPACE_BEGIN

struct Block {
	Block* next_;
	byte data_[1];
};

class MemPool {
private:
	size_t block_size_;
	size_t capacity_;
	byte* buf_;
	Block* free_;

public:
	MemPool(size_t size, size_t cap);
	~MemPool();

	void* getBuffer();
	void putBack(void* data);
};

NAMESPACE_END

#endif