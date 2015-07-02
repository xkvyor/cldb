#include <vector>
#include <cstring>
#include "gtest/gtest.h"
#include "mempool.h"

class MemPoolTest: public ::testing::Test {};

TEST_F(MemPoolTest, Test) {
	size_t size = 1024;
	size_t count = 1024;
	cl::MemPool* mem = new cl::MemPool(size, count);
	std::vector<void*> v;
	for (size_t i = 0; i < count * 2; ++i)
	{
		void* data = mem->getBuffer();
		if (i >= count)
		{
			EXPECT_EQ(data, (void*)NULL);
		}
		else
		{
			EXPECT_NE(data, (void*)NULL);
			v.push_back(data);
		}
	}
	for (auto i : v)
	{
		mem->putBack(i);
	}
	delete mem;
}

int main(int argc, char *argv[])
{
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}