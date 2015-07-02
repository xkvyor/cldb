#ifndef _CACHE_H_
#define _CACHE_H_

#include "common.h"

NAMESPACE_BEGIN

struct LruNode {
	LruNode* prev_;
	LruNode* next_;
	page_id_t page_id_;
	void* page_;
};

class LruList {
private:
	LruNode* head_;
	LruNode* tail_;

public:
	LruList(): head_(NULL), tail_(NULL)
	{}

	inline LruNode* getHead() { return head_; }
	inline LruNode* getTail() { return tail_; }

	inline void pushBack(LruNode* node)
	{
		node->prev_ = node->next_ = NULL;
		if (tail_)
		{
			tail_->next_ = node;
			node->prev_ = tail_;
			tail_ = node;
		}
		else
		{
			head_ = tail_ = node;
		}
	}

	inline void pushFront(LruNode* node)
	{
		node->prev_ = node->next_ = NULL;
		if (head_)
		{
			head_->prev_ = node;
			node->next_ = head_;
			head_ = node;
		}
		else
		{
			head_ = tail_ = node;
		}
	}

	inline void popBack()
	{
		if (tail_)
		{
			tail_ = tail_->prev_;
			if (tail_)
			{
				tail_->next_ = NULL;
			}
			else
			{
				head_ = NULL;
			}
		}
	}

	inline void popFront()
	{
		if (head_)
		{
			head_ = head_->next_;
			if (head_)
			{
				head_->prev_ = NULL;
			}
			else
			{
				tail_ = NULL;
			}
		}
	}

	inline void remove(LruNode* node)
	{
		if (node->prev_)
		{
			node->prev_->next_ = node->next_;
		}
		if (node->next_)
		{
			node->next_->prev_ = node->prev_;
		}
		if (node == head_)
		{
			head_ = node->next_;
		}
		if (node == tail_)
		{
			tail_ = node->prev_;
		}
		node->prev_ = node->next_ = NULL;
	}
};

struct HashNode {
	HashNode* next_;
	HashNode* next_hash_;
	HashNode* prev_hash_;
	hash_t hash_;
	page_id_t page_id_;
	LruNode* node_;
};

struct HashNodeBlock {
	HashNodeBlock* next_;
	HashNode* nodes_;
	HashNodeBlock(size_t size)
	{
		nodes_ = new HashNode[size];
	}
	~HashNodeBlock()
	{
		if (nodes_)
		{
			delete [] nodes_;
		}
	}
};

class HashTable {
public:
	static const size_t default_size = 64;
	static hash_t hashFunc(page_id_t page_id);

private:
	HashNode** buckets_;
	HashNode* nodes_;
	HashNode* free_nodes_;
	HashNodeBlock* blocks_;
	size_t used_;
	size_t capacity_;

	void prepareNodes(size_t size);
	HashNode* newHashNode(page_id_t page_id, hash_t h, LruNode* node);
	HashNode** findPos(page_id_t page_id, hash_t h);

public:
	HashTable(size_t size = HashTable::default_size);
	~HashTable();

	void resize(size_t size);

	LruNode* get(page_id_t page_id);
	void set(page_id_t page_id, LruNode* node);
	void del(page_id_t page_id);
};

class CacheSynchronizer {
protected:
	virtual void syncCache(page_id_t page_id, void* page) = 0;
	friend class Cache;
};

class Cache {
private:
	HashTable* hash_table_;
	LruList* list_;
	LruList* free_list_;
	LruNode* nodes_;
	size_t cache_size_;
	CacheSynchronizer* sync_;

	void promote(LruNode* node);
	void release();

public:
	Cache(size_t cache_size);
	~Cache();

	void* get(page_id_t page_id);
	void put(page_id_t page_id, void* page);
	void del(page_id_t page_id);

	inline size_t getCacheSize() { return cache_size_; }
	inline void setCacheSync(CacheSynchronizer* sync) { sync_ = sync; }
};

NAMESPACE_END

#endif