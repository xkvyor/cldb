#include <cstring>
#include "gtest/gtest.h"
#include "storage.h"

class StorageTest: public ::testing::Test {};

TEST_F(StorageTest, Test) {
	const char* filename = ".tmp.db";
	cl::DBConfig config(filename);
	cl::Storage* store = new cl::Storage(config);

	void* page = store->acquire(1);
	EXPECT_EQ(page, (void*)NULL);

	cl::page_id_t pid = store->getNewPage();
	EXPECT_EQ(pid, (cl::page_id_t)1);

	page = store->acquire(pid);
	EXPECT_NE(page, (void*)NULL);

	const char* msg = "hello world";
	memcpy(page, msg, strlen(msg)+1);
	store->release(pid);
	delete store;

	store = new cl::Storage(filename);
	page = store->acquire(pid);
	EXPECT_EQ(0, memcmp(msg, page, strlen(msg)+1));
	store->release(pid);

	pid = store->getNewPage();
	EXPECT_EQ(pid, (cl::page_id_t)2);

	page = store->acquire(pid);
	EXPECT_NE(page, (void*)NULL);
	memcpy(page, msg, strlen(msg)+1);
	store->release(pid);
	delete store;
}

int main(int argc, char *argv[])
{
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
