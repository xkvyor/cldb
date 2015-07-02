#include "file.h"
#include "mempool.h"
#include "storage.h"

NAMESPACE_BEGIN

Storage::Storage(const char* filename):
	file_(NULL), cache_(NULL), mem_(NULL), valid_(false)
{
	lock();

	file_ = new File(filename);
	if (file_->exist())
	{
		if (file_->open() && loadFile() && meta_.magic_ == DB_MAGIC)
		{
			goto file_ok;
		}
	}
	else if (file_->create())
	{
		DBConfig config(NULL);
		initNewFileHeader(config);
		goto file_ok;
	}

	goto end;

file_ok:
	valid_ = initCache();

end:
	unlock();
}

Storage::Storage(const DBConfig& config):
	file_(NULL), cache_(NULL), valid_(false)
{
	lock();

	file_ = new File(config.filename_);
	if (file_->create())
	{
		initNewFileHeader(config);
		valid_ = initCache();
	}
	else
	{
		deletePtr(file_);
	}

	unlock();
}

Storage::~Storage()
{
	lock();

	if (valid_)
	{
		valid_ = false;
		syncMeta();
	}

	deletePtr(cache_);
	deletePtr(mem_);
	deletePtr(file_);

	unlock();
}

bool Storage::loadFile()
{
	return (file_->read(0, &meta_, sizeof(meta_)) == sizeof(meta_));
}

void Storage::initNewFileHeader(const DBConfig& config)
{
	zeroMemory(&meta_, sizeof(meta_));
	meta_.magic_ = DB_MAGIC;
	meta_.page_size_ = config.page_size_;
	meta_.cache_size_ = config.cache_size_;
	meta_.min_items_ = config.min_items_;
}

bool Storage::initCache()
{
	cache_ = new Cache(meta_.cache_size_);
	cache_->setCacheSync(this);
	mem_ = new MemPool(meta_.page_size_, meta_.cache_size_+1);
	return (cache_ && mem_);
}

void Storage::syncMeta()
{
	file_->write(0, &meta_, sizeof(meta_));
}

void Storage::lock()
{
	lock_.lock();
}

void Storage::unlock()
{
	lock_.unlock();
}

void* Storage::acquire(page_id_t page_id)
{
	if (page_id > meta_.max_page_id_)
	{
		return NULL;
	}

	lock();

	void* page = cache_->get(page_id);
	if (page)
	{
		return page;
	}

	page = mem_->getBuffer();
	file_->read(pageOffset(page_id), page, meta_.page_size_);
	cache_->put(page_id, page);
	return page;
}

void Storage::release(page_id_t page_id)
{
	unlock();
}

void* Storage::getPage(page_id_t page_id)
{
	if (page_id > meta_.max_page_id_)
	{
		return NULL;
	}

	void* page = cache_->get(page_id);
	if (page)
	{
		return page;
	}

	page = mem_->getBuffer();
	file_->read(pageOffset(page_id), page, meta_.page_size_);
	cache_->put(page_id, page);
	return page;
}


page_id_t Storage::getNewPage()
{
	lock();
	page_id_t ret = ++meta_.max_page_id_;
	unlock();
	return ret;
}

void Storage::syncCache(page_id_t page_id, void* page)
{
	file_->write(pageOffset(page_id), page, meta_.page_size_);
	mem_->putBack(page);
}

offset_t Storage::pageOffset(page_id_t page_id)
{
	return DB_HEADER_SIZE + (page_id - 1) * meta_.page_size_;
}

void Storage::storeOverflowData(const void* data, size_t size)
{
	lock();

	void* page;

	if (getOverflowPageId() == 0)
	{
		meta_.overflow_page_ = getNewPage();
		meta_.overflow_offset_ = sizeof(OverflowPageHeader);
		page = getPage(meta_.overflow_page_);
		((OverflowPageHeader*)page)->next_ = 0;
		((OverflowPageHeader*)page)->off_ = sizeof(OverflowPageHeader);
	}

	while(size > 0)
	{
		page_id_t pid = getOverflowPageId();
		offset_t off = getOverflowPageOffset();

		size_t write_bytes = min(getPageSize() - off, size);

		void* page = getPage(pid);
		memcpy((byte*)page+off, data, write_bytes);

		data = (byte*)data + write_bytes;
		size -= write_bytes;

		meta_.overflow_offset_ += write_bytes;

		if ((uint32_t)meta_.overflow_offset_ == getPageSize())
		{
			meta_.overflow_page_ = getNewPage();
			meta_.overflow_offset_ = sizeof(OverflowPageHeader);
			((OverflowPageHeader*)page)->next_ = meta_.overflow_page_;
			page = getPage(meta_.overflow_page_);
			((OverflowPageHeader*)page)->next_ = 0;
			((OverflowPageHeader*)page)->off_ = sizeof(OverflowPageHeader);
		}
	}

	unlock();
}

NAMESPACE_END