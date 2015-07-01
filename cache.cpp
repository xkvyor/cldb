#include "cache.h"

NAMESPACE_BEGIN

HashTable::HashTable(size_t size):
	buckets_(NULL), nodes_(NULL), free_nodes_(NULL),
	blocks_(NULL), used_(0), capacity_(0)
{
	resize(size);
}

HashTable::~HashTable()
{
	deleteArray(buckets_);
	while(blocks_)
	{
		HashNodeBlock* tmp = blocks_;
		blocks_ = blocks_->next_;
		deletePtr(tmp);
	}
}

void HashTable::prepareNodes(size_t size)
{
	HashNodeBlock* b = new HashNodeBlock(size);
	b->next_ = blocks_;
	blocks_ = b;
	HashNode* nodes = b->nodes_;
	for (int i = size-2; i >= 0; --i)
	{
		nodes[i].next_ = &nodes[i+1];
	}
	nodes[size-1].next_ = free_nodes_;
	free_nodes_ = nodes;
}

HashNode* HashTable::newHashNode(page_id_t page_id, hash_t h, LruNode* node)
{
	HashNode* ret = free_nodes_;
	free_nodes_ = free_nodes_->next_;
	ret->next_ = NULL;
	ret->prev_hash_ = NULL;
	ret->next_hash_ = nodes_;
	if (nodes_)
	{
		nodes_->prev_hash_ = ret;
	}
	nodes_ = ret;
	ret->page_id_ = page_id;
	ret->hash_ = h;
	ret->node_ = node;
	return ret;
}

void HashTable::resize(size_t size)
{
	HashNode** new_buckets = new HashNode*[size];
	zeroMemory(new_buckets, size * sizeof(HashNode*));
	prepareNodes(size - capacity_);
	HashNode* cur = nodes_;
	while(cur)
	{
		cur->next_ = new_buckets[cur->hash_ & (size - 1)];
		new_buckets[cur->hash_ & (size - 1)] = cur;
		cur = cur->next_hash_;
	}
	deleteArray(buckets_);
	buckets_ = new_buckets;
	capacity_ = size;
}

HashNode** HashTable::findPos(page_id_t page_id, hash_t h)
{
	HashNode** pos = &buckets_[h & (capacity_ - 1)];
	while(*pos && (*pos)->page_id_ != page_id)
	{
		pos = &(*pos)->next_;
	}
	return pos;
}

hash_t HashTable::hashFunc(page_id_t page_id)
{
	hash_t h = page_id;
	h *= 0x1b873593;
	h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;
    return h;
}

LruNode* HashTable::get(page_id_t page_id)
{
	hash_t h = HashTable::hashFunc(page_id);
	HashNode** pos = findPos(page_id, h);
	return (*pos) ? (*pos)->node_ : NULL;
}

void HashTable::set(page_id_t page_id, LruNode* node)
{
	hash_t h = HashTable::hashFunc(page_id);
	HashNode** pos = findPos(page_id, h);
	if (*pos)
	{
		(*pos)->node_ = node;
	}
	else
	{
		if (used_ == capacity_)
		{
			resize(capacity_ << 1);
		}
		pos = findPos(page_id, h);
		(*pos) = newHashNode(page_id, h, node);
		++used_;
	}
}

void HashTable::del(page_id_t page_id)
{
	hash_t h = HashTable::hashFunc(page_id);
	HashNode** pos = findPos(page_id, h);
	if (*pos)
	{
		HashNode* d = *pos;
		*pos = (*pos)->next_;
		if (d->prev_hash_)
		{
			d->prev_hash_->next_hash_ = d->next_hash_;
		}
		if (d->next_hash_)
		{
			d->next_hash_->prev_hash_ = d->prev_hash_;
		}
		d->next_ = free_nodes_;
		free_nodes_ = d;
		--used_;
	}
}

Cache::Cache(size_t cache_size):
	hash_table_(NULL), list_(NULL), free_list_(NULL), nodes_(NULL),
	cache_size_(cache_size), sync_(NULL)
{
	hash_table_ = new HashTable();
	list_ = new LruList();
	free_list_ = new LruList();
	nodes_ = new LruNode[cache_size];
	for (size_t i = 0; i < cache_size; ++i)
	{
		free_list_->pushBack(&nodes_[i]);
	}
}

Cache::~Cache()
{
	deletePtr(hash_table_);
	LruNode* cur = NULL;
	while(sync_ && (cur = list_->getHead()))
	{
		sync_->syncCache(cur->page_id_, cur->page_);
		list_->popFront();
	}
	deletePtr(list_);
	deletePtr(free_list_);
	deleteArray(nodes_);
}

void Cache::promote(LruNode* node)
{
	list_->remove(node);
	list_->pushFront(node);
}

void Cache::release()
{
	LruNode* node = list_->getTail();
	list_->popBack();
	if (sync_)
	{
		sync_->syncCache(node->page_id_, node->page_);
	}
	hash_table_->del(node->page_id_);
	free_list_->pushBack(node);
}

void* Cache::get(page_id_t page_id)
{
	LruNode* node = hash_table_->get(page_id);
	if (node)
	{
		promote(node);
		return node->page_;
	}
	else
	{
		return NULL;
	}
}

void Cache::put(page_id_t page_id, void* page)
{
	LruNode* node = hash_table_->get(page_id);
	if (node)
	{
		node->page_ = page;
		promote(node);
	}
	else
	{
		if (free_list_->getHead() == NULL)
		{
			release();
		}
		node = free_list_->getHead();
		free_list_->popFront();
		node->page_id_ = page_id;
		node->page_ = page;
		list_->pushFront(node);
		hash_table_->set(page_id, node);
	}
}

void Cache::del(page_id_t page_id)
{
	LruNode* node = hash_table_->get(page_id);
	if (node)
	{
		list_->remove(node);
		if (sync_)
		{
			sync_->syncCache(node->page_id_, node->page_);
		}
		free_list_->pushBack(node);
		hash_table_->del(page_id);
	}
}

NAMESPACE_END
