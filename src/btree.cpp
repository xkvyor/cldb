#include "btree.h"
#include "storage.h"

NAMESPACE_BEGIN

BTree::BTree(Storage* store):
	store_(store), page_size_(store_->getPageSize()),
	data_buf_(NULL), cmp_buf_(NULL), split_buf_(NULL)
{
	if (store_->getRootId() == 0)
	{
		buildRootPage(BTreePageType::Leaf);
		store_->storeOverflowData(NULL, 0);
	}

	data_buf_ = new Buffer();
	cmp_buf_ = new Buffer();
	split_buf_ = new Buffer();
}

BTree::~BTree()
{
	deletePtr(data_buf_);
	deletePtr(cmp_buf_);
	deletePtr(split_buf_);
}

static inline void setPageType(void* page, BTreePageType type);
static inline BTreePageType getPageType(void* page);
static inline size_t getPageSpace(void* page);

static inline void setLeafPageId(void* page, page_id_t page_id);
static inline page_id_t getLeafPageId(void* page);
static inline void setLeafParent(void* page, page_id_t page_id);
static inline page_id_t getLeafParent(void* page);
static inline void setLeafPrev(void* page, page_id_t page_id);
static inline page_id_t getLeafPrev(void* page);
static inline void setLeafNext(void* page, page_id_t page_id);
static inline page_id_t getLeafNext(void* page);
static inline void setLeafOffset(void* page, offset_t off);
static inline offset_t getLeafOffset(void* page);
static inline void setLeafItemOffset(void* page, uint32_t index, offset_t off);
static inline offset_t getLeafItemOffset(void* page, uint32_t index);

static inline void setLeafItemCount(void* page, size_t count);
static inline size_t getLeafItemCount(void* page);
static inline void setInternalPageId(void* page, page_id_t page_id);
static inline page_id_t getInternalPageId(void* page);
static inline void setInternalParent(void* page, page_id_t page_id);
static inline page_id_t getInternalParent(void* page);
static inline void setInternalOffset(void* page, offset_t off);
static inline offset_t getInternalOffset(void* page);
static inline void setInternalItemCount(void* page, size_t count);
static inline size_t getInternalItemCount(void* page);
static inline void setInternalItemOffset(void* page, uint32_t index, offset_t off);
static inline offset_t getInternalItemOffset(void* page, uint32_t index);
static inline page_id_t getLastChild(void* page);
static inline page_id_t getChild(void* page, uint32_t i);

void setPageType(void* page, BTreePageType type)
{
	((LeafPageHeader*)page)->type_ = type;
}

BTreePageType getPageType(void* page)
{
	return ((LeafPageHeader*)page)->type_;
}

size_t getPageSpace(void* page)
{
	BTreePageType type = getPageType(page);
	if (type == BTreePageType::Internal)
	{
		return getInternalOffset(page) - 
			(sizeof(InternalPageHeader) + getInternalItemCount(page) * sizeof(offset_t));
	}
	else if (type == BTreePageType::Leaf)
	{
		return getLeafOffset(page) -
			(sizeof(LeafPageHeader) + getLeafItemCount(page) * sizeof(offset_t));
	}
	else
	{
		return 0;
	}
}

void setLeafPageId(void* page, page_id_t page_id)
{
	((LeafPageHeader*)page)->page_id_ = page_id;
}

page_id_t getLeafPageId(void* page)
{
	return ((LeafPageHeader*)page)->page_id_;
}

void setLeafParent(void* page, page_id_t page_id)
{
	((LeafPageHeader*)page)->parent_id_ = page_id;
}

page_id_t getLeafParent(void* page)
{
	return ((LeafPageHeader*)page)->parent_id_;
}

void setLeafPrev(void* page, page_id_t page_id)
{
	((LeafPageHeader*)page)->prev_ = page_id;
}

page_id_t getLeafPrev(void* page)
{
	return ((LeafPageHeader*)page)->prev_;
}

void setLeafNext(void* page, page_id_t page_id)
{
	((LeafPageHeader*)page)->next_ = page_id;
}

page_id_t getLeafNext(void* page)
{
	return ((LeafPageHeader*)page)->next_;
}

void setLeafOffset(void* page, offset_t off)
{
	((LeafPageHeader*)page)->hoff_ = off;
}

offset_t getLeafOffset(void* page)
{
	return ((LeafPageHeader*)page)->hoff_;
}

void setLeafItemCount(void* page, size_t count)
{
	((LeafPageHeader*)page)->item_count_ = count;
}

size_t getLeafItemCount(void* page)
{
	return ((LeafPageHeader*)page)->item_count_;
}

void setLeafItemOffset(void* page, uint32_t index, offset_t off)
{
	*(offset_t*)((byte*)page + sizeof(LeafPageHeader) + sizeof(offset_t) * index) = off;
}

offset_t getLeafItemOffset(void* page, uint32_t index)
{
	return *(offset_t*)((byte*)page + sizeof(LeafPageHeader) + sizeof(offset_t) * index);
}

void setInternalPageId(void* page, page_id_t page_id)
{
	((InternalPageHeader*)page)->page_id_ = page_id;
}

page_id_t getInternalPageId(void* page)
{
	return ((InternalPageHeader*)page)->page_id_;
}

void setInternalParent(void* page, page_id_t page_id)
{
	((InternalPageHeader*)page)->parent_id_ = page_id;
}

page_id_t getInternalParent(void* page)
{
	return ((InternalPageHeader*)page)->parent_id_;
}

void setInternalOffset(void* page, offset_t off)
{
	((InternalPageHeader*)page)->hoff_ = off;
}

offset_t getInternalOffset(void* page)
{
	return ((InternalPageHeader*)page)->hoff_;
}

void setInternalItemCount(void* page, size_t count)
{
	((InternalPageHeader*)page)->item_count_ = count;
}

size_t getInternalItemCount(void* page)
{
	return ((InternalPageHeader*)page)->item_count_;
}

void setInternalItemOffset(void* page, uint32_t index, offset_t off)
{
	*(offset_t*)((byte*)page + sizeof(InternalPageHeader) + sizeof(offset_t) * index) = off;
}

offset_t getInternalItemOffset(void* page, uint32_t index)
{
	return *(offset_t*)((byte*)page + sizeof(InternalPageHeader) + sizeof(offset_t) * index);
}

page_id_t getLastChild(void* page)
{
	return ((InternalPageHeader*)page)->last_child_id_;
}

void setLastChild(void* page, page_id_t page_id)
{
	((InternalPageHeader*)page)->last_child_id_ = page_id;
}

page_id_t getChild(void* page, uint32_t i)
{
	if (i == getInternalItemCount(page))
	{
		return getLastChild(page);
	}
	return *(page_id_t*)((byte*)page + getInternalItemOffset(page, i));
}

void setChild(void* page, uint32_t i, page_id_t page_id)
{
	if (i == getInternalItemCount(page))
	{
		setLastChild(page, page_id);
	}
	else
	{
		*(page_id_t*)((byte*)page + getInternalItemOffset(page, i)) = page_id;
	}
}

void BTree::buildRootPage(BTreePageType type)
{
	store_->lock();
	page_id_t rid = store_->getNewPage();
	store_->setRootId(rid);
	void* page = store_->acquire(rid);
	zeroMemory(page, page_size_);
	setPageType(page, type);
	if (type == BTreePageType::Internal)
	{
		setInternalPageId(page, rid);
		setInternalOffset(page, page_size_);
	}
	else
	{
		setLeafPageId(page, rid);
		setLeafOffset(page, page_size_);
	}
	store_->unlock();
}

size_t BTree::get(const void* key, size_t ksize, void* buf, size_t size)
{
	store_->lock();

	get(key, ksize);

	size_t ret = min(size, data_buf_->getSize());
	memcpy(buf, data_buf_->getBuffer(), ret);

	store_->unlock();

	return ret;
}

size_t BTree::get(const void* key, size_t ksize, void** val)
{
	store_->lock();

	get(key, ksize);

	void* data = (void*)(new byte[data_buf_->getSize()]);
	size_t ret = data_buf_->getSize();
	memcpy(data, data_buf_->getBuffer(), ret);
	*val = data;

	store_->unlock();

	return ret;
}

void BTree::get(const void* key, size_t ksize)
{
	page_id_t page_id;
	uint32_t i;
	bool found = rec_search(key, ksize, &page_id, &i);
	if (!found)
	{
		data_buf_->clear();
		return;
	}
	getItem(page_id, i+1, data_buf_);
}

bool BTree::rec_search(const void* key, size_t ksize, page_id_t* page_id, uint32_t* index)
{
	page_id_t cur = store_->getRootId();
	void* page;
	bool found;
	uint32_t i;

	for (;;)
	{
		page = store_->getPage(cur);
		found = search(page, key, ksize, &i);
		if (getPageType(page) == BTreePageType::Internal)
		{
			cur = getChild(page, i + (found ? 1 : 0));
		}
		else
		{
			*page_id = cur;
			*index = i;
			break;
		}
	}

	return found;
}

bool BTree::search(void* page, const void* key, size_t ksize, uint32_t* index)
{
	BTreePageType type = getPageType(page);
	int32_t step = (type == BTreePageType::Leaf ? 2 : 1);
	int32_t left = 0;
	int32_t right = (type == BTreePageType::Leaf ?
				getLeafItemCount(page) :
				getInternalItemCount(page)) - step;
	int32_t r, mid;
	bool found = false;

	while (left <= right)
	{
		mid = (type == BTreePageType::Leaf ?
			((left + right) >> 1) & ~0x1 :
			(left + right) >> 1);

		r = compare(page, mid, key, ksize);
		if (r == 0)
		{
			left = mid;
			found = true;
			break;
		}
		else if (r < 0)
		{
			left = mid + step;
		}
		else
		{
			right = mid - step;
		}
	}

	*index = left;
	return found;
}

int32_t BTree::compare(void* page, uint32_t index, const void* key, size_t ksize)
{
	getItem(page, index, cmp_buf_);
	int32_t r = memcmp(cmp_buf_->getBuffer(), key, min(ksize, cmp_buf_->getSize()));
	if (r == 0)
	{
		return cmp_buf_->getSize() - ksize;
	}
	return r;
}

void BTree::getItem(page_id_t page_id, uint32_t index, Buffer* buf)
{
	void* page = store_->getPage(page_id);
	getItem(page, index, buf);
}

void BTree::getItem(void* page, uint32_t index, Buffer* buf)
{
	BTreePageType type = getPageType(page);
	byte* p;
	buf->clear();
	if (type == BTreePageType::Internal)
	{
		p = (byte*)page + getInternalItemOffset(page, index) + sizeof(page_id_t);
	}
	else
	{
		p = (byte*)page + getLeafItemOffset(page, index);
	}
	if (getItemType(p) == ItemType::OnPage)
	{
		buf->append(p + sizeof(OnPageItemHeader), ((OnPageItemHeader*)p)->size_);
	}
	else
	{
		size_t total = ((OffPageItemHeader*)p)->total_size_;
		buf->append(p + sizeof(OffPageItemHeader), getItemSizeOnPage(p) - sizeof(OffPageItemHeader));
		total -= getItemSizeOnPage(p) - sizeof(OffPageItemHeader);
		page_id_t pid = ((OffPageItemHeader*)p)->ov_page_id_;
		offset_t off = ((OffPageItemHeader*)p)->ov_off_;
		while (total > 0)
		{
			void* page = store_->getPage(pid);
			size_t read_bytes = min(page_size_-off, total);
			buf->append((byte*)page + off, read_bytes);
			total -= read_bytes;
			if (total <= 0)
			{
				break;
			}
			pid = ((OverflowPageHeader*)page)->next_;
			off = sizeof(OverflowPageHeader);
			page = store_->getPage(pid);
		}
	}
}

void BTree::put(const void* key, size_t ksize, const void* val, size_t vsize)
{
	page_id_t page_id;
	uint32_t i;
	bool found;

	for (;;)
	{
		found = rec_search(key, ksize, &page_id, &i);
		if (found)
		{
			if (modifyLeafItem(page_id, i+1, val, vsize))
			{
				break;
			}
			else
			{
				remove(key, ksize);
				continue;
			}
		}
		else
		{
			if (insertLeafItem(page_id, i, key, ksize, val, vsize))
			{
				break;
			}
		}
		splitLeaf(page_id);
		// iter();
	}
}

bool BTree::modifyLeafItem(page_id_t page_id, uint32_t index, const void* val, size_t vsize)
{
	void* page = store_->getPage(page_id);
	byte* p = (byte*)page + getLeafItemOffset(page, index);
	ItemType itype = getItemType(p);
	if (itype == ItemType::OnPage && ((OnPageItemHeader*)p)->size_ >= vsize)
	{
		setItemLocalDataSize(p, vsize);
		memcpy(p + sizeof(OnPageItemHeader), val, vsize);
	}
	else if (itype == ItemType::OffPage && ((OffPageItemHeader*)p)->total_size_ >= vsize)
	{
		size_t write_bytes = min(getItemSizeOnPage(p) - sizeof(OffPageItemHeader), vsize);
		((OffPageItemHeader*)p)->total_size_ = vsize;
		setItemLocalDataSize(p, write_bytes);
		memcpy(p+sizeof(OffPageItemHeader), val, write_bytes);
		val = (byte*)val + write_bytes;
		vsize -= write_bytes;
		page_id_t pid = ((OffPageItemHeader*)p)->ov_page_id_;
		offset_t off = ((OffPageItemHeader*)p)->ov_off_;
		while(vsize > 0)
		{
			void* page = store_->getPage(pid);
			write_bytes = min(page_size_ - off, vsize);
			memcpy((byte*)page+off, val, write_bytes);
			val = (byte*)val + write_bytes;
			vsize -= write_bytes;
			pid = *(page_id_t*)page;
			off = sizeof(OverflowPageHeader);
		}
	}
	else
	{
		return false;
	}
	return true;
}

bool BTree::insertLeafItem(page_id_t page_id, uint32_t index,
						const void* key, size_t ksize,
						const void* val, size_t vsize)
{
	void* page = store_->getPage(page_id);
	size_t off_page_size = adjustAlign(((page_size_ - sizeof(LeafPageHeader)) / store_->getMinItems())
									 - sizeof(offset_t) - ALIGN_WIDTH);
	size_t on_page_key_size = sizeof(OnPageItemHeader) + adjustAlign(ksize);
	size_t on_page_val_size = sizeof(OnPageItemHeader) + adjustAlign(vsize);
	size_t need = min(on_page_key_size, off_page_size) + min(on_page_val_size, off_page_size);

	if (need + (sizeof(offset_t) << 1) > getPageSpace(page))
	{
		return false;
	}

	size_t n = getLeafItemCount(page);
	for (uint32_t i = n+1; i > index+1; --i)
	{
		setLeafItemOffset(page, i, getLeafItemOffset(page, i-2));
	}
	setLeafItemCount(page, n+2);
	setLeafOffset(page, getLeafOffset(page)-need);
	setLeafItemOffset(page, index, getLeafOffset(page));
	setLeafItemOffset(page, index+1, getLeafOffset(page)+min(on_page_key_size, off_page_size));

	byte* p = (byte*)page + getLeafItemOffset(page, index);
	writeItem(p, key, ksize);
	p = (byte*)page + getLeafItemOffset(page, index+1);
	writeItem(p, val, vsize);

	return true;
}

void BTree::writeItem(byte* p, const void* data, size_t size)
{
	size_t off_page_size = adjustAlign(((page_size_ - sizeof(LeafPageHeader)) / store_->getMinItems())
									 - sizeof(offset_t) - ALIGN_WIDTH);
	size_t on_page_size = sizeof(OnPageItemHeader) + adjustAlign(size);

	if (on_page_size <= off_page_size)
	{
		setItemType(p, ItemType::OnPage);
		setItemLocalDataSize(p, size);
		memcpy(p+sizeof(OnPageItemHeader), data, size);
	}
	else
	{
		setItemType(p, ItemType::OffPage);
		((OffPageItemHeader*)p)->ov_page_id_ = store_->getOverflowPageId();
		((OffPageItemHeader*)p)->ov_off_ = store_->getOverflowPageOffset();
		((OffPageItemHeader*)p)->total_size_ = size;
		size_t local_size = off_page_size - sizeof(OffPageItemHeader);
		setItemLocalDataSize(p, local_size);
		memcpy(p+sizeof(OffPageItemHeader), data, local_size);
		store_->storeOverflowData((byte*)data+local_size, size-local_size);
	}
}

void BTree::insertInternalItem(page_id_t pid, page_id_t lid, page_id_t rid,
							void* item, size_t size)
{
	void* page = store_->getPage(pid);
	uint32_t index;

	{
		split_buf_->clear();
		byte* p = (byte*)item;
		if (getItemType(p) == ItemType::OnPage)
		{
			split_buf_->append(p + sizeof(OnPageItemHeader), ((OnPageItemHeader*)p)->size_);
		}
		else
		{
			size_t total = ((OffPageItemHeader*)p)->total_size_;
			split_buf_->append(p + sizeof(OffPageItemHeader), getItemSizeOnPage(p) - sizeof(OffPageItemHeader));
			total -= getItemSizeOnPage(p) - sizeof(OffPageItemHeader);
			page_id_t pid = ((OffPageItemHeader*)p)->ov_page_id_;
			offset_t off = ((OffPageItemHeader*)p)->ov_off_;
			while (total > 0)
			{
				void* page = store_->getPage(pid);
				size_t read_bytes = min(page_size_-off, total);
				split_buf_->append((byte*)page + off, read_bytes);
				total -= read_bytes;
				if (total <= 0)
				{
					break;
				}
				pid = ((OverflowPageHeader*)page)->next_;
				off = sizeof(OverflowPageHeader);
				page = store_->getPage(pid);
			}
		}
	}

	{
		int32_t left = 0;
		int32_t right = getInternalItemCount(page) - 1;
		int32_t r, mid;

		while (left <= right)
		{
			mid = (left + right) >> 1;

			r = compare(page, mid, split_buf_->getBuffer(), split_buf_->getSize());

			if (r < 0)
			{
				left = mid + 1;
			}
			else
			{
				right = mid - 1;
			}
		}

		index = left;
	}

	size_t need = size + sizeof(page_id_t);
	size_t n = getInternalItemCount(page);
	for (uint32_t i = n; i > index; --i)
	{
		setInternalItemOffset(page, i, getInternalItemOffset(page, i-1));
	}

	setInternalItemCount(page, n+1);
	setInternalOffset(page, getInternalOffset(page)-need);
	setInternalItemOffset(page, index, getInternalOffset(page));

	byte* dst = (byte*)page+getInternalItemOffset(page, index);
	memcpy(dst, &lid, sizeof(page_id_t));
	memcpy(dst+sizeof(page_id_t), item, size);

	setChild(page, index+1, rid);
}

void BTree::splitLeaf(page_id_t page_id)
{
	void* page = store_->getPage(page_id);

	page_id_t split_page_id = store_->getNewPage();
	void* split_page = store_->getPage(split_page_id);

	page_id_t parent = getLeafParent(page);
	setPageType(split_page, BTreePageType::Leaf);
	setLeafPageId(split_page, split_page_id);
	if (parent == 0)
	{
		buildRootPage(BTreePageType::Internal);
		parent = store_->getRootId();
		setLeafParent(page, parent);
	}
	setLeafParent(split_page, parent);
	setLeafPrev(split_page, page_id);
	setLeafNext(split_page, getLeafNext(page));
	setLeafNext(page, split_page_id);
	setLeafOffset(split_page, page_size_);
	setLeafItemCount(split_page, 0);

	size_t n = getLeafItemCount(page);
	uint32_t mid = (n >> 1) & ~0x1;
	byte* p = (byte*)page + getLeafItemOffset(page, mid);
	size_t size = getItemSizeOnPage(p);

	void* parent_page = store_->getPage(parent);
	if (size + sizeof(offset_t) + sizeof(page_id_t) > getPageSpace(parent_page))
	{
		splitInternal(parent);
		setLeafParent(split_page, getLeafParent(page));
	}

	for (uint32_t i = mid, j = 0; i < n; ++i, ++j)
	{
		p = (byte*)page + getLeafItemOffset(page, i);
		size = getItemSizeOnPage(p);
		setLeafOffset(split_page, getLeafOffset(split_page)-size);
		setLeafItemOffset(split_page, j, getLeafOffset(split_page));
		memcpy((byte*)split_page+getLeafOffset(split_page), p, size);
	}
	setLeafItemCount(split_page, n-mid);
	setLeafItemCount(page, mid);

	p = (byte*)split_page + getLeafItemOffset(split_page, 0);
	insertInternalItem(getLeafParent(page), page_id, split_page_id,
					p, getItemSizeOnPage(p));

	compactLeafPage(page);
}

void BTree::splitInternal(page_id_t page_id)
{
	void* page = store_->getPage(page_id);
	page_id_t split_page_id = store_->getNewPage();
	void* split_page = store_->getPage(split_page_id);

	page_id_t parent = getInternalParent(page);
	setPageType(split_page, BTreePageType::Internal);
	setInternalPageId(split_page, split_page_id);
	if (parent == 0)
	{
		buildRootPage(BTreePageType::Internal);
		parent = store_->getRootId();
		setInternalParent(page, parent);
	}
	setInternalParent(split_page, parent);
	setInternalOffset(split_page, page_size_);
	setLastChild(split_page, getLastChild(page));
	setInternalItemCount(split_page, 0);

	size_t n = getInternalItemCount(page);
	byte* p = (byte*)page + getInternalItemOffset(page, n >> 1) + sizeof(page_id_t);
	size_t size = getItemSizeOnPage(p);

	void* parent_page = store_->getPage(parent);
	if (size + sizeof(offset_t) + sizeof(page_id_t) > getPageSpace(parent_page))
	{
		splitInternal(parent);
		setInternalParent(split_page, getInternalParent(page));
	}

	uint32_t i, j;
	void* cp;
	for (i = (n >> 1) + 1, j = 0; i < n; ++i, ++j)
	{
		p = (byte*)page + getInternalItemOffset(page, i);
		cp = store_->getPage(*(page_id_t*)p);
		if (getPageType(cp) == BTreePageType::Internal)
		{
			setInternalParent(cp, split_page_id);
		}
		else
		{
			setLeafParent(cp, split_page_id);
		}
		size = getItemSizeOnPage(p+sizeof(page_id_t)) + sizeof(page_id_t);
		setInternalOffset(split_page, getInternalOffset(split_page)-size);
		setInternalItemOffset(split_page, j, getInternalOffset(split_page));
		memcpy((byte*)split_page+getInternalOffset(split_page), p, size);
	}
	cp = store_->getPage(getLastChild(page));
	if (getPageType(cp) == BTreePageType::Internal)
	{
		setInternalParent(cp, split_page_id);
	}
	else
	{
		setLeafParent(cp, split_page_id);
	}
	setInternalItemCount(split_page, j);
	setInternalItemCount(page, n >> 1);
	p = (byte*)page + getInternalItemOffset(page, n >> 1);
	setLastChild(page, *(page_id_t*)p);

	p = (byte*)page + getInternalItemOffset(page, n >> 1) + sizeof(page_id_t);
	insertInternalItem(getInternalParent(page), page_id, split_page_id,
					p, getItemSizeOnPage(p));

	compactInternalPage(page);
}

void BTree::compactLeafPage(void* page)
{
	data_buf_->clear();
	size_t n = getLeafItemCount(page);
	size_t size;
	byte* item;

	for (uint32_t i = 0; i < n; ++i)
	{
		item = (byte*)page + getLeafItemOffset(page, i);
		size = getItemSizeOnPage(item);
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
		setLeafItemOffset(page, i, off);
	}

	setLeafOffset(page, off);
}

void BTree::compactInternalPage(void* page)
{
	data_buf_->clear();
	size_t n = getInternalItemCount(page);
	size_t size;
	byte* item;

	for (uint32_t i = 0; i < n; ++i)
	{
		item = (byte*)page + getInternalItemOffset(page, i);
		size = getItemSizeOnPage(item+sizeof(page_id_t)) + sizeof(page_id_t);
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
		setInternalItemOffset(page, i, off);
	}

	setInternalOffset(page, off);
}

void BTree::remove(const void* key, size_t ksize)
{
	page_id_t page_id;
	uint32_t i;
	bool found = rec_search(key, ksize, &page_id, &i);
	if (!found)
	{
		return;
	}
	removeItem(page_id, i);
}

void BTree::removeItem(page_id_t page_id, uint32_t index)
{
	void* page = store_->getPage(page_id);
	setLeafItemCount(page, getLeafItemCount(page)-2);
	size_t n = getLeafItemCount(page);
	for (uint32_t i = index; i < n; ++i)
	{
		setLeafItemOffset(page, i, getLeafItemOffset(page, i+2));
	}
	compactLeafPage(page);
}

void BTree::traverse(Iterator* iter)
{
	page_id_t pid = store_->getRootId();
	void* page = store_->getPage(pid);
	while(getPageType(page) == BTreePageType::Internal)
	{
		pid = getChild(page, 0);
		page = store_->getPage(pid);
	}

	Buffer* kbuf = new Buffer();
	Buffer* vbuf = new Buffer();

	for(;;)
	{
		for (uint32_t i = 0; i < getLeafItemCount(page); i+=2)
		{
			getItem(page, i, kbuf);
			getItem(page, i+1, vbuf);
			iter->process(kbuf->getBuffer(), kbuf->getSize(), vbuf->getBuffer(), vbuf->getSize());
		}
		pid = getLeafNext(page);
		if (pid == 0)
		{
			break;
		}
		else
		{
			page = store_->getPage(pid);
		}
	}

	deletePtr(kbuf);
	deletePtr(vbuf);
}

NAMESPACE_END