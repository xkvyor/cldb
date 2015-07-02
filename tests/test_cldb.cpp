#include <cstring>
#include "gtest/gtest.h"
#include "cldb.h"

class DBTest: public ::testing::Test {};

TEST_F(DBTest, Test) {
	const char* filename = ".db";
	cl::DBConfig config(filename, true);
	cl::DB* db = new cl::DB();
	db->open(config);

	int count = 1000000;

	for (int i = 0; i < count; ++i)
	{
		db->put(&i, sizeof(i), &i, sizeof(i));
	}

	int r;
	size_t l;
	for (int i = 0; i < count; ++i)
	{
		l = db->get(&i, sizeof(i), &r, sizeof(r));
		EXPECT_EQ(l, sizeof(i));
		EXPECT_EQ(r, i);
	}

	for (int i = 0; i < count; i+=2)
	{
		db->del(&i, sizeof(i));
	}

	for (int i = 0; i < count; ++i)
	{
		l = db->get(&i, sizeof(i), &r, sizeof(r));
		if (i & 0x1)
		{
			EXPECT_EQ(l, sizeof(i));
			EXPECT_EQ(r, i);
		}
		else
		{
			EXPECT_EQ(l, 0u);
		}
	}

	delete db;
}

int main(int argc, char *argv[])
{
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
