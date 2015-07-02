#ifndef _CLDB_H_
#define _CLDB_H_

#include "common.h"

NAMESPACE_BEGIN

class Storage;
class BTree;

class DB {
private:
	Storage* store_;
	BTree* tree_;

	bool valid_;

public:
	DB();
	~DB();

	bool open(const char* filename);
	bool open(const DBConfig& config);
	bool create(const DBConfig& config);
	size_t get(const void* key, size_t ksize, void* buf, size_t buf_size);
	void put(const void* key, size_t ksize, const void* val, size_t vsize);
	void del(const void* key, size_t ksize);
	void close();

	inline bool valid() { return valid_; }
};

NAMESPACE_END

#endif