#ifndef _HASH_H_
#define _HASH_H_

#include "common.h"

NAMESPACE_BEGIN

enum HashPageType {
	Index = 0,
	Bucket = 1
};

struct IndexPageHeader {
	HashPageType type_;
	uint32_t level_;
	uint32_t next_split_;
	uint32_t per_;
};

struct BucketPageHeader {
	HashPageType type_;
	page_id_t prev_;
	page_id_t next_;
	page_id_t overflow_;
	offset_t off_;
	uint32_t item_count_;
};

hash_t murmur3(const void* data, size_t size, uint32_t seed);

class Storage;
class Buffer;

class Hash : public DBInterface {
private:
	Storage* store_;
	size_t page_size_;

	Buffer* data_buf_;
	Buffer* cmp_buf_;
	Buffer* stack_;

	page_id_t initRootPage();
	bool search(const void* page, const void* key, size_t ksize, uint32_t* index);
	int compare(const void* page, uint32_t index, const void* key, size_t ksize);
	void getItem(const void* page, uint32_t index, Buffer* buf);
	void get(const void* key, size_t ksize, Buffer* buf);
	void split();
	void newBucket(page_id_t bid);
	void compact(void* page);
	page_id_t locateBucket(uint32_t n);
	page_id_t newOverflowBucket(page_id_t pid);

	void modifyItem(void* page, uint32_t index, const void* data, size_t size);
	void insertPair(void* page, uint32_t index, hash_t h,
					const void* key, size_t ksize,
					const void* val, size_t vsize);
	void removePair(void* page, uint32_t index);
	void writeItem(void* p, hash_t h, const void* data, size_t size);

public:
	Hash(Storage* store);
	~Hash();

	size_t get(const void* key, size_t ksize,
		void* buf, size_t size);
	size_t get(const void* key, size_t ksize, void** val);
	void put(const void* key, size_t ksize, const void* val, size_t vsize);
	void remove(const void* key, size_t ksize);

	void traverse(Iterator* iter);
};

NAMESPACE_END

#endif