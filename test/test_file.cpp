#include "gtest/gtest.h"
#include "file.h"

class FileTest: public ::testing::Test {};

TEST_F(FileTest, Operations) {
	const char* filename = ".fileop_test_file";
	cl::File f(filename);
	EXPECT_EQ(f.exist(), false);
	EXPECT_EQ(f.create(), true);
	EXPECT_EQ(f.close(), true);
	EXPECT_EQ(f.exist(), true);
	EXPECT_EQ(f.open(), true);
	EXPECT_EQ(f.close(), true);
	EXPECT_EQ(f.remove(), true);
	EXPECT_EQ(f.exist(), false);
}

TEST_F(FileTest, ReadWrite) {
	const char* filename = "fileop.test";
	cl::File* fp = new cl::File(filename);
	EXPECT_EQ(fp->exist(), false);
	EXPECT_EQ(fp->create(), true);
	EXPECT_EQ(fp->exist(), true);
	const char* msg = "hello world!";
	cl::offset_t off = 100;
	EXPECT_EQ(strlen(msg), fp->write(off, msg, strlen(msg)));
	EXPECT_EQ(fp->close(), true);
	EXPECT_EQ(fp->open(), true);
	char buf[1024] = {0};
	EXPECT_EQ(strlen(msg), fp->read(off, buf, strlen(msg)));
	EXPECT_EQ(strcmp(msg, buf), 0);
	EXPECT_EQ(fp->close(), true);
	EXPECT_EQ(fp->remove(), true);
	EXPECT_EQ(fp->exist(), false);
	delete fp;
}

int main(int argc, char *argv[])
{
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}