#include "file.h"
#include "storage.h"
#include "btree.h"
#include "cldb.h"

NAMESPACE_BEGIN

DB::DB(): store_(NULL), tree_(NULL), valid_(false)
{}

DB::~DB()
{
	deletePtr(tree_);
	deletePtr(store_);
}

bool DB::open(const char* filename)
{
	if (store_)
	{
		return false;
	}

	File f(filename);
	if (!f.exist())
	{
		return false;
	}
	f.close();

	store_ = new Storage(filename);
	if (!store_->valid())
	{
		deletePtr(store_);
		return false;
	}

	tree_ = new BTree(store_);
	return true;
}

bool DB::open(const DBConfig& config)
{
	if (store_)
	{
		return false;
	}

	if (!config.create_)
	{
		return open(config.filename_);
	}

	File f(config.filename_);
	if (!f.exist())
	{
		f.close();
		return create(config);
	}
	f.close();

	return open(config.filename_);
}

bool DB::create(const DBConfig& config)
{
	if (store_)
	{
		return false;
	}

	store_ = new Storage(config);
	if (!store_->valid())
	{
		deletePtr(store_);
		return false;
	}

	tree_ = new BTree(store_);
	return true;
}

size_t DB::get(const void* key, size_t ksize, void* buf, size_t buf_size)
{
	if (tree_)
	{
		return tree_->get(key, ksize, buf, buf_size);
	}
	else
	{
		return 0;
	}
}

void DB::put(const void* key, size_t ksize, const void* val, size_t vsize)
{
	if (tree_)
	{
		return tree_->put(key, ksize, val, vsize);
	}
}

void DB::del(const void* key, size_t ksize)
{
	if (tree_)
	{
		tree_->remove(key, ksize);
	}
}

void DB::close()
{
	valid_ = false;
	deletePtr(tree_);
	deletePtr(store_);
}

NAMESPACE_END