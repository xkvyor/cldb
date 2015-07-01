# clDB
clDB is a key-value database based on B+-tree.
### Example
  `
  #include "cldb.h"
  `

	int main(int argc, char const *argv[])
	{
		const char* filename = "example.db";
		cl::DBConfig config(filename, true);
		cl::DB* db = new cl::DB();
		db->open(config);

		int count = 100000;

		for (int i = 0; i < count; ++i)
		{
			db->put(&i, sizeof(i), &i, sizeof(i));
		}

		int r;
		size_t l;
		for (int i = 0; i < count; ++i)
		{
			l = db->get(&i, sizeof(i), &r, sizeof(r));
		}

		for (int i = 0; i < count; i+=2)
		{
			db->del(&i, sizeof(i));
		}

		for (int i = 0; i < count; ++i)
		{
			l = db->get(&i, sizeof(i), &r, sizeof(r));
		}

		delete db;
		
		return 0;
	}
