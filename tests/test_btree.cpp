#include <cstdlib>
#include <ctime>
#include <cstring>
#include <string>
#include <vector>
#include "gtest/gtest.h"
#include "btree.h"
#include "storage.h"

using std::string;
using std::vector;

class BTreeTest: public ::testing::Test {};

void randstr(size_t min_size, size_t max_size, string& ret)
{
	int len = rand() % (max_size - min_size + 1) + min_size;
	ret.resize(len);
	for (int i = 0; i < len; ++i)
	{
		ret[i] = 'a' + rand() % 26;
	}
}

TEST_F(BTreeTest, Test) {
	const char* filename = ".btree.db";
	cl::DBConfig config(filename);
	cl::Storage* store = new cl::Storage(config);
	cl::BTree* tree = new cl::BTree(store);

	int count = 1000;

	size_t mlen = 1;
	size_t slen = 15000;
	string key, val;
	vector<string> kv, vv;

	for (int i = 0; i < count; ++i)
	{
		randstr(mlen, slen, key);
		randstr(mlen, slen, val);
		// printf("put %d:%d\n", key.length(), val.length());
		tree->put(key.data(), key.length(), val.data(), val.length());
		kv.push_back(key);
		vv.push_back(val);

		// tree->iter();
	}

	char buf[slen];
	for (int i = 0; i < count; ++i)
	{
		size_t l = tree->get(kv[i].c_str(), kv[i].length(), buf, slen);
		EXPECT_EQ(l, vv[i].length());
		EXPECT_EQ(memcmp(buf, vv[i].c_str(), l), 0);
	}

	for (int i = 0; i < count; i+=2)
	{
		randstr(mlen, slen, val);
		vv[i] = val;
		tree->put(kv[i].c_str(), kv[i].length(), val.c_str(), val.length());

		// tree->iter();
	}

	for (int i = 0; i < count; ++i)
	{
		size_t l = tree->get(kv[i].c_str(), kv[i].length(), buf, slen);
		EXPECT_EQ(l, vv[i].length());
		EXPECT_EQ(memcmp(buf, vv[i].c_str(), l), 0);
		// buf[l] = '\0';
		// printf("%s\n", buf);
		// printf("%s\n", vv[i].c_str());
	}

	for (int i = 0; i < count; i+=2)
	{
		tree->remove(kv[i].c_str(), kv[i].length());
	}

	for (int i = 0; i < count; ++i)
	{
		size_t l = tree->get(kv[i].c_str(), kv[i].length(), buf, slen);
		if (i & 0x1)
		{
			EXPECT_EQ(l, vv[i].length());
			EXPECT_EQ(memcmp(buf, vv[i].c_str(), l), 0);
		}
		else
		{
			EXPECT_EQ(l, 0u);
		}
	}

	delete tree;
	delete store;
}

int main(int argc, char *argv[])
{
	srand(time(NULL));
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
