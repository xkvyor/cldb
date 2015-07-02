#ifndef _STORAGE_H_
#define _STORAGE_H_

#include <mutex>
#include "common.h"
#include "cache.h"

NAMESPACE_BEGIN

struct BufferNode {
	BufferNode* next_;
	void* buf_;
};

class Buffer {
private:
	static const size_t default_size = 64;

private:
	byte* buf_;
	size_t used_;
	size_t capacity_;

	void resize(size_t size);

public:
	Buffer();
	~Buffer();

	void clear();
	void append(const void* data, size_t size);

	inline byte* getBuffer() { return buf_; }
	inline size_t getSize() { return used_; }
};

class File;
class MemPool;

class Storage: public CacheSynchronizer {
private:
	DBHeader meta_;
	File* file_;
	Cache* cache_;
	MemPool* mem_;
	bool valid_;

	std::recursive_mutex lock_;

	bool loadFile();
	void initNewFileHeader(const DBConfig& config);
	bool initCache();
	void syncMeta();
	void syncCache(page_id_t page_id, void* page);

	inline offset_t pageOffset(page_id_t page_id);

public:
	Storage(const char* filename);
	Storage(const DBConfig& config);
	virtual ~Storage();

	void lock();
	void unlock();

	void* acquire(page_id_t page_id);
	void release(page_id_t page_id);
	void* getPage(page_id_t page_id);
	page_id_t getNewPage();

	void storeOverflowData(const void* data, size_t size);

	inline bool valid() { return valid_; }

	inline DBType getType() { return meta_.type_; }
	inline size_t getPageSize() { return meta_.page_size_; }
	inline size_t getMinItems() { return meta_.min_items_; }
	inline page_id_t getRootId() { return meta_.root_id_; }
	inline void setRootId(page_id_t root) { meta_.root_id_ = root; }
	inline page_id_t getOverflowPageId() { return meta_.overflow_page_; }
	inline offset_t getOverflowPageOffset() { return meta_.overflow_offset_; }
};

NAMESPACE_END

#endif