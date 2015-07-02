#ifndef _COMMON_H_
#define _COMMON_H_

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <cstdio>

#define NAMESPACE_BEGIN namespace cl {
#define NAMESPACE_END }

NAMESPACE_BEGIN

typedef uint8_t byte;
typedef uint32_t offset_t;
typedef uint32_t hash_t;
typedef uint32_t page_id_t;

// align
const uint32_t ALIGN_WIDTH = 4;
inline size_t adjustAlign(size_t n)
{
	return (n + ALIGN_WIDTH - 1) & (~(ALIGN_WIDTH - 1));
}

inline void zeroMemory(void* data, size_t size)
{
	memset(data, 0, size);
}

template<class T>
inline void deletePtr(T& p)
{
	if (p)
	{
		delete p;
		p = NULL;
	}
}

template<class T>
inline void deleteArray(T& p)
{
	if (p)
	{
		delete [] p;
		p = NULL;
	}
}

template<class T>
inline T min(T a, T b)
{
	return a < b ? a : b;
}

template<class T>
inline T max(T a, T b)
{
	return a > b ? a : b;
}

static const uint32_t DB_MAGIC = 0x97f59c92;

enum DBType {
	BTreeDB = 0,
	HashDB = 1
};

struct DBHeader {
	uint32_t magic_;
	DBType type_;
	size_t page_size_;
	size_t cache_size_;
	page_id_t max_page_id_;
	page_id_t overflow_page_;
	offset_t overflow_offset_;
	page_id_t free_pages_;
	size_t min_items_;
	page_id_t root_id_;
};

static const size_t DB_HEADER_SIZE = sizeof(DBHeader);

class DBConfig {
private:
	static const bool default_create = false;
	static const size_t default_page_size = 4096;
	static const size_t default_cache_size = 512;
	static const size_t default_min_items = 4;

public:
	const char* filename_;
	bool create_;
	DBType type_;
	size_t page_size_;
	size_t cache_size_;
	size_t min_items_;

public:
	DBConfig(const char* fn,
		bool create = DBConfig::default_create,
		DBType type = DBType::BTreeDB,
		size_t page_size = DBConfig::default_page_size,
		size_t cache_size = DBConfig::default_cache_size,
		size_t min_items = DBConfig::default_min_items):
		filename_(fn), create_(create),
		type_(type), page_size_(page_size),
		cache_size_(cache_size), min_items_(min_items)
	{}
};

struct OverflowPageHeader {
	page_id_t next_;
	offset_t off_;
};

class Iterator {
public:
	virtual void process(const void* key, size_t ksize, const void* val, size_t vsize) = 0;
};

NAMESPACE_END

#endif