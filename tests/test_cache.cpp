#include <vector>
#include <cstring>
#include "gtest/gtest.h"
#include "cache.h"

class HashTableTest: public ::testing::Test {};

TEST_F(HashTableTest, Test) {
	cl::HashTable* table = new cl::HashTable();
	int count = 100000;
	for (int i = 0; i < count; ++i)
	{
		table->set(i, (cl::LruNode*)i);
	}
	for (int i = 0; i < count; ++i)
	{
		EXPECT_EQ((cl::LruNode*)i, table->get(i));
	}
	for (int i = 0; i < count; i+=2)
	{
		table->set(i, (cl::LruNode*)(i<<1));
	}
	for (int i = 0; i < count; ++i)
	{
		if (i & 1)
			EXPECT_EQ((cl::LruNode*)i, table->get(i));
		else
			EXPECT_EQ((cl::LruNode*)(i<<1), table->get(i));
	}
	for (int i = 0; i < count; i+=3)
	{
		table->del(i);
	}
	for (int i = 0; i < count; ++i)
	{
		if (i % 3)
		{
			EXPECT_NE((cl::LruNode*)NULL, table->get(i));
		}
		else
		{
			EXPECT_EQ(NULL, table->get(i));
		}
	}
	delete table;
}

class CacheTest: public ::testing::Test {};

TEST_F(CacheTest, Operations) {
	size_t cache_size = 512;
	cl::Cache cache(cache_size);
	EXPECT_EQ(cache.getCacheSize(), cache_size);

	for (uint32_t i = 0; i <= cache_size; ++i)
	{
		cache.put(i, (void*)i);
	}
	EXPECT_EQ(cache.get((uint32_t)0), (void*)0);
	EXPECT_EQ(cache.get((uint32_t)cache_size), (void*)cache_size);
	cache.del((uint32_t)cache_size);
	EXPECT_EQ(cache.get((uint32_t)cache_size), (void*)0);
	for (uint32_t i = 0; i < cache_size; ++i)
	{
		EXPECT_EQ(cache.get((uint32_t)i), (void*)i);
	}
}

TEST_F(CacheTest, LRU) {
	cl::Cache cache(3);
	char buf[1024];
	std::vector<uint32_t> v = { 1, 2, 3, 1, 4, 1, 3, 5 };
	for (auto i: v)
	{
		cache.put(i, buf);
	}
	EXPECT_NE(cache.get((uint32_t)1), (void*)0);
	EXPECT_EQ(cache.get((uint32_t)2), (void*)0);
	EXPECT_NE(cache.get((uint32_t)3), (void*)0);
	EXPECT_EQ(cache.get((uint32_t)4), (void*)0);
	EXPECT_NE(cache.get((uint32_t)5), (void*)0);
}

int main(int argc, char *argv[])
{
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}