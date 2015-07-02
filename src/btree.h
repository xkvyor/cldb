#ifndef _BTREE_H_
#define _BTREE_H_

#include "common.h"

NAMESPACE_BEGIN

// (c1) k1 [c2) ... kn [last)

enum PageType {
	Internal = 0,
	Leaf = 1
};

struct InternalPageHeader {
	PageType type_;
	page_id_t page_id_;
	page_id_t parent_id_;
	offset_t hoff_;
	page_id_t last_child_id_;
	size_t item_count_;
};

struct LeafPageHeader {
	PageType type_;
	page_id_t page_id_;
	page_id_t parent_id_;
	page_id_t prev_;
	page_id_t next_;
	offset_t hoff_;
	size_t item_count_;
};

struct FreePageHeader {
	page_id_t page_id_;
	page_id_t next_;
};

enum ItemType {
	OnPage = 0,
	OffPage = 1
};

struct OnPageItemHeader {
	// ItemType type_;
	size_t size_;
};

struct OffPageItemHeader {
	// ItemType type_;
	size_t local_size_;
	size_t total_size_;
	page_id_t ov_page_id_;
	offset_t ov_off_;
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

class Storage;

typedef void (*iterfunc)(const void* data, size_t size);

class BTree {
private:
	Storage* store_;
	size_t page_size_;
	Buffer data_buf_;
	Buffer cmp_buf_;
	Buffer split_buf_;

	void buildRootPage(PageType type);
	void get(const void* key, size_t ksize);
	bool rec_search(const void* key, size_t ksize, page_id_t* page_id, uint32_t* index);
	bool search(void* page, const void* key, size_t ksize, uint32_t* index);
	void getItem(page_id_t page_id, uint32_t index, Buffer& buf);
	void getItem(void* page, uint32_t index, Buffer& buf);
	int32_t compare(void* page, uint32_t index, const void* key, size_t ksize);

	bool modifyLeafItem(page_id_t page_id, uint32_t index, const void* val, size_t vsize);
	bool insertLeafItem(page_id_t page_id, uint32_t index,
						const void* key, size_t ksize,
						const void* val, size_t vsize);
	void insertInternalItem(page_id_t pid, page_id_t lid, page_id_t rid,
						void* item, size_t size);

	void splitLeaf(page_id_t page_id);
	void splitInternal(page_id_t page_id);
	void compactLeafPage(void* page);
	void compactInternalPage(void* page);
	void removeItem(page_id_t page_id, uint32_t index);
	void writeItem(byte* p, const void* data, size_t size);

public:
	BTree(Storage* store);
	~BTree();

	size_t get(const void* key, size_t ksize,
		void* buf, size_t size);
	size_t get(const void* key, size_t ksize, void** val);
	void put(const void* key, size_t ksize, const void* val, size_t vsize);
	void remove(const void* key, size_t ksize);

	void traverse(Iterator* iter);
};

NAMESPACE_END

#endif