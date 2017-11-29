#ifndef _CFG_H__
#define _CFG_H__

#include <cstdio>
#include <thread>
#include <atomic>
#include <mutex>
#include <chrono>
#include <cassert>
#include <iostream>
#include <map>
#include <algorithm>
#include <memory>
#include <vector>
#include <list>
#include <deque>
#include <cstdio>
#include <array>
#include <system_error>
#include <sys/stat.h>
#include <functional>
#include <fstream>
#include <sstream>
#include <time.h>
#include <set>
#include <cstring>
#include <typeinfo>
#include <string>
#include <cstdlib>
#include <ctime>
#include <chrono>
#include <dirent.h>
#include <cstdint>
#include <unistd.h>
#include <utility>
#include <bzlib.h>
#include "ukey.h"
#include "quote-1017-lxl.h"
#include "GetUkey.h"
#include "SDS20Struct-2017.h"
#include "CreatePath.h"
#include "Compare.h"

//#define COMPLETION
#define DEBUG
#define PATH  "/UKData/TDF/fqy/QUOTE/"
//#define TOREQUEUEST
using namespace std;
using namespace chrono;
using namespace chronos;

struct FILEHEAD {
	char maintype[32];
	int  main_shmid;
	int  data_shmid;
	int  bytes_per_record;
	int  recnum_per_block;
	int  updatetm;
	int  recnum;
	char flags[4]; // 固定为shm //
	int  version;  // 目前为101 //
};

enum Type
{
	LVT = 1,
	CFE,
	IDX,
	SPI,
	ORD,
	ORQ,
	TRD,
};

void SH_IndexCodeTrans(char *ChCode);

/*时间相关*/
int64_t GetMsTime(int64_t ymd, int64_t hmsu);
template<int TYPE>
string FindFileName(string Path)
{
	DIR *	dir = nullptr;
	struct	dirent *ptr = nullptr;
	map<int, string> type{
		{ Type::LVT,"LVT" },
		{ Type::CFE,"CFE" },
		{ Type::IDX,"IDX" },
		{ Type::SPI,"SPI" },
		{ Type::ORD,"ORD" },
		{ Type::ORQ,"ORQ" },
		{ Type::TRD,"TRD" },
	};

	if ((dir = opendir(Path.c_str())) == nullptr)
	{
		cerr << "BasePath is inexist" << endl;;
		return string();
	}
	while ((ptr = readdir(dir)) != nullptr)
	{
		if (ptr->d_type == 8)
		{
			auto ret = strncasecmp(type[TYPE].c_str(), ptr->d_name, 3);
			if (0 == ret)
			{
				string filename = Path;
				filename += "/";
				filename += ptr->d_name;
				return filename;
			}
			else
			{
				continue;
			}
		}
		else
		{
			continue;
		}
	}
	return string();
}

/*将数据转换为字符串*/
void Snapshot2str(Snapshot *ptr, string &str);
void Order2str(Order *ptr, string &str);
void Trans2str(Transaction* ptr, string &str);
void Orderque2str(OrderQueue *ptr, string &str);
/*将内存块数据转换为对应的回放数据*/
bool LvtToSnapshot(Snapshot& OutPut, SDS20LEVEL2& Input);
bool IdxToSnapshot(Snapshot& OutPut, SDS20INDEX& InPut);
bool Cfe_SpiToSnapshot(Snapshot& OutPut, SDS20FUTURE& InPut);
bool TrdToTransaction(Transaction& OutPut, SDS20TRANSACTION& InPut);
bool OrdToOrder(Order& OutPut, SDS20ORDER& InPut);
bool OrqToOrderqueue(OrderQueue& OutPut, SDS20ORDERQUEUE& InPut);
/*将tdb数据转换为回放数据*/
bool DeCompress(const std::string& filename, vector<string>& Vec);

void TdbToSnapshot(int64_t ukey, vector<string>& Vec_Str, list<shared_ptr<Snapshot>>& L_Snap);
void TdbToOrder(int64_t ukey, vector<string>& Vec_Str, list<shared_ptr<Order>>& L_Snap);
void TdbToOrderQueue(int64_t ukey, vector<string>& Vec_Str, list<shared_ptr<OrderQueue>>& L_Snap);
void TdbToTransaction(int64_t ukey, vector<string>& Vec_Str, list<shared_ptr<Transaction>>& L_Snap);

bool merger_sort(map<string, int64_t> FileList, int64_t EndTime, string FileName);
void MySort(list<Snapshot> &v_data, string FileName);
#endif