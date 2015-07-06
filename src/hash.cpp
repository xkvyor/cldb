#include "hash.h"
#include "storage.h"

NAMESPACE_BEGIN

hash_t murmur3(const void* data, size_t size, uint32_t seed)
{
	uint32_t c1 = 0xcc9e2d51;
	uint32_t c2 = 0x1b873593;
	int r1 = 15;
	int r2 = 13;
	int m = 5;
	uint32_t n = 0xe6546b64;
 
    hash_t h = seed;
    uint32_t k = 0;

    const uint32_t* end = (const uint32_t*)((const uint8_t*)data + (size & ~0x3));
    for (const uint32_t* begin = (const uint32_t*)data;
    	begin < end; ++begin)
    {
    	k = *begin;

    	k *= c1;
    	k = (k << r1) | (k >> (32-r1));
    	k *= c2;

    	h = h ^ k;
    	h = (h << r2) | (h >> (32-r2));
    	h = h * m + n;
    }

    k = 0;
    const uint8_t* remain = (const uint8_t*)end;
    switch(size & 0x3)
    {
    	case 3:
    	k ^= remain[2] << 16;
    	case 2:
    	k ^= remain[1] << 8;
    	case 1:
    	k ^= remain[0];
    	k *= c1;
    	k = (k << r1) | (k >> (32-r1));
    	k *= c2;

    	h ^= k;
    }

    h ^= size;

    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;

    return h;
}

Hash::Hash(Storage* store):
	store_(store), page_size_(store_->getPageSize()),
	data_buf_(NULL), cmp_buf_(NULL), stack_(NULL)
{
	store_->lock();

	if (store_->getRootId() == 0)
	{
		store_->setRootId(initRootPage());
	}

	data_buf_ = new Buffer();
	cmp_buf_ = new Buffer();
	stack_ = new Buffer();

	store_->unlock();
}

Hash::~Hash()
{
	store_ = NULL;
	deletePtr(data_buf_);
	deletePtr(cmp_buf_);
	deletePtr(stack_);
}

static inline void initIndexPage(void* page);
static inline void initBucketPage(void* page, size_t page_size);

static inline HashPageType getPageType(const void* page);
static inline void setPageType(void* page, HashPageType type);

static inline uint32_t getLevel(const void* page);
static inline void setLevel(void* page, uint32_t level);
static inline uint32_t getNextSplit(const void* page);
static inline void setNextSplit(void* page, uint32_t next);
static inline uint32_t getBucketsPerPage(const void* page);
static inline void setBucketsPerPage(void* page, uint32_t per);

static inline page_id_t getPrevBucket(const void* page);
static inline void setPrevBucket(void* page, page_id_t page_id);
static inline page_id_t getNextBucket(const void* page);
static inline void setNextBucket(void* page, page_id_t page_id);
static inline offset_t getBucketOffset(const void* page);
static inline void setBucketOffset(void* page, offset_t off);
static inline uint32_t getItemCount(const void* page);
static inline void setItemCount(void* page, uint32_t count);
static inline size_t getBucketSpace(const void* page);
static inline page_id_t getOverflowBucket(const void* page);
static inline void setOverflowBucket(void* page, page_id_t pid);

static inline page_id_t getIndexPageId(const void* page, uint32_t index);
static inline void setIndexPageId(void* page, uint32_t index, page_id_t page_id);
static inline offset_t getHashItemOffset(const void* page, uint32_t index);
static inline void setHashItemOffset(void* page, uint32_t index, offset_t off);
static inline uint32_t getHash(const void* page, uint32_t index);
static inline void* getItemPointer(const void* page, uint32_t index);

void initIndexPage(void* page)
{
	setPageType(page, HashPageType::Index);
	setLevel(page, 0);
	setNextSplit(page, 0);
	setBucketsPerPage(page, 1);
}

void initBucketPage(void* page, size_t page_size)
{
	setPageType(page, HashPageType::Bucket);
	setPrevBucket(page, 0);
	setNextBucket(page, 0);
	setOverflowBucket(page, 0);
	setBucketOffset(page, page_size);
	setItemCount(page, 0);
}

HashPageType getPageType(const void* page)
{
	return *(HashPageType*)page;
}

void setPageType(void* page, HashPageType type)
{
	*(HashPageType*)page = type;
}

size_t getMaxBuckets(size_t page_size)
{
	return (page_size - sizeof(IndexPageHeader)) / sizeof(page_id_t);
}

uint32_t getLevel(const void* page)
{
	return ((IndexPageHeader*)page)->level_;
}

void setLevel(void* page, uint32_t level)
{
	((IndexPageHeader*)page)->level_ = level;
}

uint32_t getNextSplit(const void* page)
{
	return ((IndexPageHeader*)page)->next_split_;
}

void setNextSplit(void* page, uint32_t next)
{
	((IndexPageHeader*)page)->next_split_ = next;
}

uint32_t getBucketsPerPage(const void* page)
{
	return ((IndexPageHeader*)page)->per_;
}

void setBucketsPerPage(void* page, uint32_t per)
{
	((IndexPageHeader*)page)->per_ = per;
}

page_id_t getPrevBucket(const void* page)
{
	return ((BucketPageHeader*)page)->prev_;
}

void setPrevBucket(void* page, page_id_t page_id)
{
	((BucketPageHeader*)page)->prev_ = page_id;
}

page_id_t getNextBucket(const void* page)
{
	return ((BucketPageHeader*)page)->next_;
}

void setNextBucket(void* page, page_id_t page_id)
{
	((BucketPageHeader*)page)->next_ = page_id;
}

offset_t getBucketOffset(const void* page)
{
	return ((BucketPageHeader*)page)->off_;
}

void setBucketOffset(void* page, offset_t off)
{
	((BucketPageHeader*)page)->off_ = off;
}

uint32_t getItemCount(const void* page)
{
	return ((BucketPageHeader*)page)->item_count_;
}

void setItemCount(void* page, uint32_t count)
{
	((BucketPageHeader*)page)->item_count_ = count;
}

size_t getBucketSpace(const void* page)
{
	return getBucketOffset(page) - sizeof(BucketPageHeader) - getItemCount(page) * sizeof(offset_t);
}

page_id_t getOverflowBucket(const void* page)
{
	return ((BucketPageHeader*)page)->overflow_;
}

void setOverflowBucket(void* page, page_id_t pid)
{
	((BucketPageHeader*)page)->overflow_ = pid;
}

page_id_t getIndexPageId(const void* page, uint32_t index)
{
	return ((page_id_t*)((byte*)page + sizeof(IndexPageHeader)))[index];
}

void setIndexPageId(void* page, uint32_t index, page_id_t page_id)
{
	((page_id_t*)((byte*)page + sizeof(IndexPageHeader)))[index] = page_id;
}

offset_t getHashItemOffset(const void* page, uint32_t index)
{
	return ((page_id_t*)((byte*)page + sizeof(BucketPageHeader)))[index];
}

void setHashItemOffset(void* page, uint32_t index, offset_t off)
{
	((page_id_t*)((byte*)page + sizeof(BucketPageHeader)))[index] = off;
}

uint32_t getHash(const void* page, uint32_t index)
{
	return *((uint32_t*)((byte*)page + getHashItemOffset(page, index)));
}

void* getItemPointer(const void* page, uint32_t index)
{
	return (byte*)page + getHashItemOffset(page, index) + sizeof(hash_t);
}

page_id_t Hash::initRootPage()
{
	page_id_t rid = store_->getNewPage();
	store_->setRootId(rid);
	void* page = store_->getPage(rid);
	initIndexPage(page);

	page_id_t bid = store_->getNewPage();
	void* bucket = store_->getPage(bid);
	initBucketPage(bucket, page_size_);

	setIndexPageId(page, 0, bid);

	return rid;
}

bool Hash::search(const void* page, const void* key, size_t ksize, uint32_t* index)
{
	size_t n = getItemCount(page);
	int left = 0;
	int right = n-2;
	bool found = false;
	int r, mid;

	while(left <= right)
	{
		mid = ((left + right) >> 1) & ~0x1;

		r = compare(page, mid, key, ksize);

		if (r == 0)
		{
			left = mid;
			found = true;
			break;
		}
		else if (r < 0)
		{
			left = mid + 2;
		}
		else
		{
			right = mid - 2;
		}
	}

	*index = left;
	return found;
}

int Hash::compare(const void* page, uint32_t index, const void* key, size_t ksize)
{
	{
		getItem(page, index, cmp_buf_);
		int r = memcmp(cmp_buf_->getBuffer(), key, min(ksize, cmp_buf_->getSize()));
		return r == 0 ? cmp_buf_->getSize() - ksize: r;
	}

	void* item = getItemPointer(page, index);
	size_t local_size = getItemLocalDataSize(item);
	int r = memcmp(getItemLocalData(item), key, min(local_size, ksize));

	if (r != 0)
	{
		return r;
	}

	if (getItemType(item) == ItemType::OnPage)
	{
		return local_size - ksize;
	}
	else
	{
		getItem(page, index, cmp_buf_);
		r = memcmp(cmp_buf_->getBuffer(), key, min(ksize, cmp_buf_->getSize()));
		return r == 0 ? cmp_buf_->getSize() - ksize : r;
	}
}

void Hash::getItem(const void* page, uint32_t index, Buffer* buf)
{
	buf->clear();

	void* item = getItemPointer(page, index);

	buf->append(getItemLocalData(item), getItemLocalDataSize(item));

	if (getItemType(item) == ItemType::OffPage)
	{
		size_t remain = ((OffPageItemHeader*)item)->total_size_ - getItemLocalDataSize(item);
		page_id_t pid = ((OffPageItemHeader*)item)->ov_page_id_;
		offset_t off = ((OffPageItemHeader*)item)->ov_off_;
		while (remain > 0)
		{
			void* page = store_->getPage(pid);
			size_t read_bytes = min(page_size_-off, remain);
			buf->append((byte*)page + off, read_bytes);
			remain -= read_bytes;
			if (remain <= 0)
			{
				break;
			}
			pid = ((OverflowPageHeader*)page)->next_;
			off = sizeof(OverflowPageHeader);
			page = store_->getPage(pid);
		}
	}
}

page_id_t Hash::locateBucket(uint32_t n)
{
	page_id_t pid = store_->getRootId();
	void* page = store_->getPage(pid);

	uint32_t per = getBucketsPerPage(page);

	while (per > 1)
	{
		pid = getIndexPageId(page, n / per);
		page = store_->getPage(pid);
		n %= per;
		per = getBucketsPerPage(page);
	}

	return getIndexPageId(page, n);
}

void Hash::get(const void* key, size_t ksize, Buffer* buf)
{
	page_id_t pid = store_->getRootId();
	void* page = store_->getPage(pid);

	uint32_t level = getLevel(page);
	uint32_t ns = getNextSplit(page);

	uint32_t h = murmur3(key, ksize, 0);
	uint32_t k = h & ((1 << level) - 1);
	if (k < ns)
	{
		k = h & ((1 << (level + 1)) - 1);
	}

	pid = locateBucket(k);
	page = store_->getPage(pid);

	bool found;
	uint32_t i;
	buf->clear();

	for (;;)
	{
		found = search(page, key, ksize, &i);

		if (found)
		{
			getItem(page, i+1, buf);
			break;
		}
		else if ((pid = getOverflowBucket(page)) != 0)
		{
			page = store_->getPage(pid);
		}
		else
		{
			break;
		}
	}
}

size_t Hash::get(const void* key, size_t ksize,
				void* buf, size_t size)
{
	store_->lock();

	get(key, ksize, data_buf_);
	size_t ret = min(data_buf_->getSize(), size);
	memcpy(buf, data_buf_->getBuffer(), ret);

	store_->unlock();

	return ret;
}

size_t Hash::get(const void* key, size_t ksize, void** val)
{
	store_->lock();

	get(key, ksize, data_buf_);
	*val = (void*)(new byte[data_buf_->getSize()]);
	size_t ret = data_buf_->getSize();
	memcpy(*val, data_buf_->getBuffer(), ret);

	store_->unlock();

	return ret;
}

void Hash::put(const void* key, size_t ksize, const void* val, size_t vsize)
{
	store_->lock();

	page_id_t pid = store_->getRootId();
	void* page = store_->getPage(pid);

	uint32_t level = getLevel(page);
	uint32_t ns = getNextSplit(page);

	hash_t h = murmur3(key, ksize, 0);
	uint32_t k = h & ((1 << level) - 1);
	if (k < ns)
	{
		k = h & ((1 << (level + 1)) - 1);
	}

	pid = locateBucket(k);
	page = store_->getPage(pid);

	size_t off_page_size = adjustAlign((page_size_ - sizeof(BucketPageHeader)) / store_->getMinItems()
						- sizeof(offset_t) - ALIGN_WIDTH);
	size_t on_page_key_size = adjustAlign(ksize + sizeof(size_t) + sizeof(hash_t));
	size_t on_page_val_size = adjustAlign(vsize + sizeof(size_t) + sizeof(hash_t));

	size_t need = min(on_page_key_size, off_page_size)
				+ min(on_page_val_size, off_page_size)
				+ (sizeof(offset_t) << 1);

	bool found;
	uint32_t i;
	bool removed = false;
	bool writed = false;
	bool need_split = false;

	for (;;)
	{
		found = search(page, key, ksize, &i);
		if (found)
		{
			if (adjustAlign(getItemSizeOnPage(getItemPointer(page, i+1)))
				>= min(on_page_val_size, off_page_size))
			{
				modifyItem(page, i+1, val, vsize);
				return;
			}
			else
			{
				removePair(page, i);
				removed = true;
			}
		}

		if (!writed && getBucketSpace(page) >= need)
		{
			insertPair(page, i, h, key, ksize, val, vsize);
			writed = true;
		}

		if (writed && removed)
		{
			break;
		}

		if (getOverflowBucket(page) != 0)
		{
			pid = getOverflowBucket(page);
			page = store_->getPage(pid);
		}
		else if (writed)
		{
			break;
		}
		else
		{
			pid = newOverflowBucket(pid);
			page = store_->getPage(pid);
			need_split = true;
		}
	}

	if (need_split)
	{
		// printf("before split:\n");
		// debug();
		split();
		// printf("after split:\n");
		// debug();
		// printf("split done\n");
	}

	store_->unlock();
}

void Hash::modifyItem(void* page, uint32_t index, const void* data, size_t size)
{
	size_t off_page_size = adjustAlign((page_size_ - sizeof(BucketPageHeader)) / store_->getMinItems()
						- sizeof(offset_t) - sizeof(hash_t) - ALIGN_WIDTH);
	size_t on_page_size = adjustAlign(size + sizeof(OnPageItemHeader));

	void* item = getItemPointer(page, index);

	if (on_page_size > off_page_size)
	{
		if (getItemType(item) == ItemType::OnPage)
		{
			setItemType(item, ItemType::OffPage);
			size_t local_size = off_page_size - sizeof(OffPageItemHeader);
			setItemLocalDataSize(item, local_size);
			((OffPageItemHeader*)item)->total_size_ = size;
			memcpy((byte*)item + sizeof(OffPageItemHeader), data, local_size);
			data = (byte*)data + local_size;
			size -= local_size;

			store_->storeOverflowData(NULL, 0);
			((OffPageItemHeader*)item)->ov_page_id_ = store_->getOverflowPageId();
			((OffPageItemHeader*)item)->ov_off_ = store_->getOverflowPageOffset();
			store_->storeOverflowData(data, size);
		}
		else
		{
			size_t local_size = off_page_size - sizeof(OffPageItemHeader);
			((OffPageItemHeader*)item)->total_size_ = size;
			memcpy((byte*)item + sizeof(OffPageItemHeader), data, local_size);
			data = (byte*)data + local_size;
			size -= local_size;

			page_id_t pid = ((OffPageItemHeader*)item)->ov_page_id_;
			offset_t off = ((OffPageItemHeader*)item)->ov_off_;
			while(size > 0)
			{
				void* page = store_->getPage(pid);
				size_t write_bytes = min(page_size_ - off, size);
				memcpy((byte*)page+off, data, write_bytes);
				data = (byte*)data + write_bytes;
				size -= write_bytes;
				pid = *(page_id_t*)page;
				off = sizeof(OverflowPageHeader);
			}
		}
	}
	else
	{
		setItemType(item, ItemType::OnPage);
		setItemLocalDataSize(item, size);
		memcpy((byte*)item + sizeof(size_t), data, size);
	}
}

void Hash::insertPair(void* page, uint32_t index, hash_t h,
					const void* key, size_t ksize,
					const void* val, size_t vsize)
{
	size_t off_page_size = adjustAlign(((page_size_ - sizeof(BucketPageHeader)) / store_->getMinItems())
									 - sizeof(offset_t) - ALIGN_WIDTH);
	size_t on_page_key_size = sizeof(OnPageItemHeader) + adjustAlign(ksize) + sizeof(hash_t);
	size_t on_page_val_size = sizeof(OnPageItemHeader) + adjustAlign(vsize) + sizeof(hash_t);
	size_t need = min(on_page_key_size, off_page_size) + min(on_page_val_size, off_page_size);

	size_t n = getItemCount(page);
	for (uint32_t i = n+1; i > index+1; --i)
	{
		setHashItemOffset(page, i, getHashItemOffset(page, i-2));
	}
	setItemCount(page, n+2);
	setBucketOffset(page, getBucketOffset(page)-need);
	setHashItemOffset(page, index, getBucketOffset(page));
	setHashItemOffset(page, index+1, getBucketOffset(page)+min(on_page_key_size, off_page_size));

	byte* p = (byte*)page + getHashItemOffset(page, index);
	writeItem(p, h, key, ksize);
	p = (byte*)page + getHashItemOffset(page, index+1);
	writeItem(p, h, val, vsize);
}

void Hash::writeItem(void* p, hash_t h, const void* data, size_t size)
{
	*(hash_t*)p = h;
	p = (byte*)p + sizeof(hash_t);

	size_t off_page_size = adjustAlign(((page_size_ - sizeof(BucketPageHeader)) / store_->getMinItems())
									 - sizeof(offset_t) - sizeof(hash_t) - ALIGN_WIDTH);
	size_t on_page_size = sizeof(OnPageItemHeader) + adjustAlign(size);

	if (on_page_size <= off_page_size)
	{
		setItemType(p, ItemType::OnPage);
		setItemLocalDataSize(p, size);
		memcpy((byte*)p+sizeof(OnPageItemHeader), data, size);
	}
	else
	{
		setItemType(p, ItemType::OffPage);
		store_->storeOverflowData(NULL, 0);
		((OffPageItemHeader*)p)->ov_page_id_ = store_->getOverflowPageId();
		((OffPageItemHeader*)p)->ov_off_ = store_->getOverflowPageOffset();
		((OffPageItemHeader*)p)->total_size_ = size;
		size_t local_size = off_page_size - sizeof(OffPageItemHeader);
		setItemLocalDataSize(p, local_size);
		memcpy((byte*)p+sizeof(OffPageItemHeader), data, local_size);
		store_->storeOverflowData((byte*)data+local_size, size-local_size);
	}
}

void Hash::removePair(void* page, uint32_t index)
{
	size_t n = getItemCount(page);
	for (uint32_t i = index; i < n - 2; ++i)
	{
		setHashItemOffset(page, i, getHashItemOffset(page, i+2));
	}
	if (n > 0)
	{
		setItemCount(page, n-2);
	}
}

page_id_t Hash::newOverflowBucket(page_id_t pid)
{
	void* page = store_->getPage(pid);
	page_id_t sid = store_->getNewPage();
	void* spage = store_->getPage(sid);

	initBucketPage(spage, page_size_);
	setPrevBucket(spage, pid);
	setNextBucket(spage, getNextBucket(page));
	setNextBucket(page, sid);
	setOverflowBucket(page, sid);

	return sid;
}

void Hash::split()
{
	page_id_t pid = store_->getRootId();
	void* page = store_->getPage(pid);

	uint32_t level = getLevel(page);
	uint32_t ns = getNextSplit(page);

	pid = locateBucket(ns);
	page = store_->getPage(pid);

	page_id_t sid = store_->getNewPage();
	page_id_t sh = sid;
	void* spage = store_->getPage(sid);

	initBucketPage(spage, page_size_);
	setPrevBucket(spage, pid);
	setNextBucket(spage, getNextBucket(page));
	setNextBucket(page, sid);

	hash_t h;
	uint32_t check = 1 << level;
	uint32_t remain = 0;
	uint32_t moved = 0;
	bool sorted = true;

	for (;;)
	{
		size_t n = getItemCount(page);
		remain = 0;

		for (uint32_t i = 0; i < n; i+=2)
		{
			h = getHash(page, i);
			if (h & check)
			{
				size_t ksize = getItemSizeOnPage(getItemPointer(page, i)) + sizeof(hash_t);
				size_t vsize = getItemSizeOnPage(getItemPointer(page, i+1)) + sizeof(hash_t);
				if (getBucketSpace(spage) < ksize + vsize + (sizeof(offset_t) << 1))
				{
					setItemCount(spage, moved);
					sid = newOverflowBucket(sid);
					spage = store_->getPage(sid);
					moved = 0;
					sorted = true;
				}
				if (!sorted)
				{
					uint32_t pos;
					getItem(page, i, data_buf_);
					search(spage, data_buf_->getBuffer(), data_buf_->getSize(), &pos);
					setItemCount(spage, getItemCount(spage) + 2);
					size_t sn = getItemCount(spage);
					for (uint32_t j = sn - 1; j > pos + 1; --j)
					{
						setHashItemOffset(spage, j, getHashItemOffset(spage, j-2));
					}
					setBucketOffset(spage, getBucketOffset(spage)-ksize);
					setHashItemOffset(spage, pos, getBucketOffset(spage));
					memcpy((byte*)spage + getBucketOffset(spage),
						(byte*)page + getHashItemOffset(page, i), ksize);
					setBucketOffset(spage, getBucketOffset(spage)-vsize);
					setHashItemOffset(spage, pos+1, getBucketOffset(spage));
					memcpy((byte*)spage + getBucketOffset(spage),
						(byte*)page + getHashItemOffset(page, i+1), vsize);
					getItem(page, i, data_buf_);
					moved += 2;
				}
				else
				{
					setBucketOffset(spage, getBucketOffset(spage)-ksize);
					setHashItemOffset(spage, moved++, getBucketOffset(spage));
					memcpy((byte*)spage + getBucketOffset(spage),
						(byte*)page + getHashItemOffset(page, i), ksize);
					setBucketOffset(spage, getBucketOffset(spage)-vsize);
					setHashItemOffset(spage, moved++, getBucketOffset(spage));
					memcpy((byte*)spage + getBucketOffset(spage),
						(byte*)page + getHashItemOffset(page, i+1), vsize);
					setItemCount(spage, getItemCount(spage) + 2);
				}
			}
			else
			{
				setHashItemOffset(page, remain++, getHashItemOffset(page, i));
				setHashItemOffset(page, remain++, getHashItemOffset(page, i+1));
			}
		}

		setItemCount(page, remain);

		compact(page);

		if (getOverflowBucket(page) != 0)
		{
			pid = getOverflowBucket(page);
			page = store_->getPage(pid);
			sorted = false;
		}
		else
		{
			break;
		}
	}

	newBucket(sh);
}

void Hash::newBucket(page_id_t bid)
{
	page_id_t pid = store_->getRootId();
	void* page = store_->getPage(pid);

	uint32_t level = getLevel(page);
	uint32_t ns = getNextSplit(page);
	uint32_t per = getBucketsPerPage(page);

	uint32_t n = ns + (1 << level);

	if (n / per >= getMaxBuckets(page_size_))
	{
		page_id_t old = store_->getRootId();
		page_id_t rid = store_->getNewPage();
		void* newp = store_->getPage(rid);
		initIndexPage(newp);
		setLevel(newp, level);
		setNextSplit(newp, ns);
		setBucketsPerPage(newp, per * getMaxBuckets(page_size_));
		setIndexPageId(newp, 0, old);
		store_->setRootId(rid);
		page = newp;
		per = getBucketsPerPage(newp);

		while (per > 1)
		{
			pid = store_->getNewPage();
			setIndexPageId(page, n/per, pid);
			n %= per;
			page = store_->getPage(pid);
			initIndexPage(page);
			per /= getMaxBuckets(page_size_);
			setBucketsPerPage(page, per);
		}
	}
	else
	{
		while (per > 1)
		{
			if (n % per == 0)
			{
				pid = store_->getNewPage();
				setIndexPageId(page, n/per, pid);
				page = store_->getPage(pid);
				initIndexPage(page);
				setBucketsPerPage(page, per / getMaxBuckets(page_size_));
			}
			else
			{
				pid = getIndexPageId(page, n / per);
				page = store_->getPage(pid);
			}
			n %= per;
			per = getBucketsPerPage(page);
		}
	}

	setIndexPageId(page, n, bid);

	page = store_->getPage(store_->getRootId());
	if (ns + 1 == (1u << level))
	{
		setNextSplit(page, 0);
		setLevel(page, level+1);
	}
	else
	{
		setNextSplit(page, ns+1);
	}
}

void Hash::compact(void* page)
{
	for (;;)
	{
		data_buf_->clear();

		size_t n = getItemCount(page);
		size_t size;
		byte* item;

		for (uint32_t i = 0; i < n; ++i)
		{
			item = (byte*)page + getHashItemOffset(page, i);
			size = getItemSizeOnPage(getItemPointer(page, i)) + sizeof(hash_t);
			data_buf_->append(&size, sizeof(size));
			data_buf_->append(item, size);
		}

		byte* buffer = data_buf_->getBuffer();
		offset_t off = page_size_;
		for (uint32_t i = 0; i < n; ++i)
		{
			size = *(size_t*)buffer;
			buffer += sizeof(size_t);
			off -= size;
			memcpy((byte*)page+off, buffer, size);
			buffer += size;
			setHashItemOffset(page, i, off);
		}

		setBucketOffset(page, off);

		page_id_t pid = getOverflowBucket(page);
		if (pid != 0)
		{
			page = store_->getPage(pid);
		}
		else
		{
			break;
		}
	}
}

void Hash::remove(const void* key, size_t ksize)
{
	store_->lock();

	page_id_t pid = store_->getRootId();
	void* page = store_->getPage(pid);

	uint32_t level = getLevel(page);
	uint32_t ns = getNextSplit(page);

	uint32_t h = murmur3(key, ksize, 0);
	uint32_t k = h & ((1 << level) - 1);
	if (k < ns)
	{
		k = h & ((1 << (level + 1)) - 1);
	}

	pid = locateBucket(k);
	page = store_->getPage(pid);

	bool found;
	uint32_t i;
	
	for (;;)
	{
		found = search(page, key, ksize, &i);
		if (found)
		{
			removePair(page, i);
		}
		else if ((pid = getOverflowBucket(page)) != 0)
		{
			page = store_->getPage(pid);
		}
		else
		{
			break;
		}
	}

	store_->unlock();
}

void Hash::traverse(Iterator* iter)
{
	store_->lock();

	page_id_t pid = locateBucket(0);
	void* page = store_->getPage(pid);

	Buffer k, v;
	uint32_t total = 0;

	while (pid != 0 && page != NULL)
	{
		// printf("page %d => %d\n", pid, getOverflowBucket(page));
		printf("page %d has %d items\n", pid, getItemCount(page));
		size_t n = getItemCount(page);
		total += n;
		for (uint32_t i = 0; i < n; i+=2)
		{
			k.clear();
			getItem(page, i, &k);
			v.clear();
			getItem(page, i+1, &v);
			iter->process(k.getBuffer(), k.getSize(), v.getBuffer(), v.getSize());
		}
		printf("\n");

		pid = getNextBucket(page);
		page = store_->getPage(pid);
	}

	printf("total item: %d\n", total);

	store_->unlock();
}

NAMESPACE_END