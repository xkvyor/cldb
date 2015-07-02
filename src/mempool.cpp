#include "mempool.h"

NAMESPACE_BEGIN

static void push(Block** list, Block* node)
{
	node->next_ = *list;
	*list = node;
}

static Block* pop(Block** list)
{
	Block* ret = *list;
	*list = (*list)->next_;
	return ret;
}

MemPool::MemPool(size_t size, size_t cap):
	block_size_(size), capacity_(cap),
	buf_(NULL), free_(NULL)
{
	buf_ = new byte[(size + sizeof(Block*)) * cap];
	for (size_t i = 0; i < cap; ++i)
	{
		Block* node = (Block*)&buf_[i * (size + sizeof(Block*))];
		push(&free_, node);
	}
}

MemPool::~MemPool()
{
	deleteArray(buf_);
}

void* MemPool::getBuffer()
{
	if (free_ == NULL)
	{
		return NULL;
	}
	Block* node = pop(&free_);
	return node->data_;
}

void MemPool::putBack(void* data)
{
	Block* node = (Block*)((byte*)data - sizeof(Block*));
	push(&free_, node);
}

NAMESPACE_END