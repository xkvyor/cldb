#include "file.h"
#include "storage.h"
#include "btree.h"
#include "hash.h"
#include "cldb.h"

NAMESPACE_BEGIN

DB::DB(): store_(NULL), db_(NULL), valid_(false)
{}

DB::~DB()
{
	deletePtr(db_);
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

	if (store_->getType() == DBType::BTreeDB)
	{
		db_ = new BTree(store_);
	}
	else if (store_->getType() == DBType::HashDB)
	{
		db_ = new Hash(store_);
	}
	else
	{
		return false;
	}

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

	if (store_->getType() == DBType::BTreeDB)
	{
		db_ = new BTree(store_);
	}
	else if (store_->getType() == DBType::HashDB)
	{
		db_ = new Hash(store_);
	}
	else
	{
		return false;
	}
	return true;
}

size_t DB::get(const void* key, size_t ksize, void* buf, size_t buf_size)
{
	if (db_)
	{
		return db_->get(key, ksize, buf, buf_size);
	}
	else
	{
		return 0;
	}
}

void DB::put(const void* key, size_t ksize, const void* val, size_t vsize)
{
	if (db_)
	{
		return db_->put(key, ksize, val, vsize);
	}
}

void DB::del(const void* key, size_t ksize)
{
	if (db_)
	{
		db_->remove(key, ksize);
	}
}

void DB::close()
{
	valid_ = false;
	deletePtr(db_);
	deletePtr(store_);
}

NAMESPACE_END