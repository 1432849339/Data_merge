#include "cfg.h"

string date;
string type;

void merge_Snapshot()
{

}

void merge_Order()
{

}

void merge_OrderQueue()
{

}

void merge_Transaction()
{

}

int main(int argc,char* argv[])
{
	if (argc < 3)
	{
		cerr << "usr:type[]  date" << endl;
		return -1;
	}
	date = argv[1];
	string type = argv[2];

	map<string, function<void()>> func{
		{ "TICK", merge_Snapshot },
		{ "ORDER", merge_Order },
		{ "ORDERQUEUE", merge_OrderQueue },
		{ "TRANS", merge_Transaction },
		//{ "SORT", snapshot_sort },
	};

    return 0;
}