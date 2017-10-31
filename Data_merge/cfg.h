#ifndef _CFG_H__
#define _CFG_H__

#include <cstdio>
#include <thread>
#include <atomic>
#include <mutex>
#include <chrono>
#include <iostream>
#include <map>
#include <algorithm>
#include <memory>
#include <vector>
#include <list>
#include <deque>
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

#define SPLIT_HROUS  19
#define SPLIT_MINN   0
#define PATH  "/data/UKData/QUOTE/"

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

void SH_IndexCodeTrans(char *ChCode);

/*时间相关*/
int64_t GetMsTime(int64_t ymd, int64_t hmsu);
time_t GetTr_time();
int GetTrday();

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

void TdbToSnapshot(int64_t ukey, vector<string>& Vec_Str, list<Snapshot>& L_Snap);
void TdbToSnapshot_include_orderque(int64_t ukey, vector<string>& Vec_Str, list<Snapshot>& L_Snap, map<int64_t, OrderQueue>& AskQue, map<int64_t, OrderQueue>& BidQue);
void TdbToOrder(int64_t ukey, vector<string>& Vec_Str, list<Order>& L_Snap);
void TdbToOrderQueue(int64_t ukey, vector<string>& Vec_Str, list<OrderQueue>& L_Snap);
void TdbToTransaction(int64_t ukey, vector<string>& Vec_Str, list<Transaction>& L_Snap);
void MakeDictionary(vector<string>& Str_orderque, int64_t ukey, map<int64_t, OrderQueue>& AskQue, map<int64_t, OrderQueue>& BidQue);
//写入行情数据
template<typename T>
bool WriteToCvs(map<int64_t, list<T>>& ukey_list_data, string& date, function<void(T*, string&)> Trans2str);
//写入回放数据
template<typename T>
bool SortAndWrite(map<int64_t, list<T>>& ukey_list_data, string& FileName, string& date);

template<typename SOURCE,typename TARGET>
class Quote_Request {
public:
	Quote_Request() = default;
	Quote_Request(Quote_Request& ref) = delete;
	void set_date(const string& date);
	bool set_data(string& filename,function<bool(TARGET&, SOURCE&)> Trans);
	bool set_FileList(string& path, string& market, string& variety);
	bool completion(function<void(int64_t, vector<string>&, list<TARGET>&)> Trans);
	bool WriteToFile(function<void(TARGET*, string&)> Trans2str);
private:
	string							 _date;
	map<int64_t, string>			  FileList;
	map<int64_t, list<TARGET>>		 _data;
};



template<typename SOURCE, typename TARGET>
inline void Quote_Request<SOURCE, TARGET>::set_date(const string & date)
{
	_data = date;
}

template<typename SOURCE, typename TARGET>
inline bool Quote_Request<SOURCE, TARGET>::set_data(string & filename,function<bool(TARGET&, SOURCE&)> Trans)
{
	FILEHEAD				FileHead{ 0 };
	array<SOURCE, 10000>	temp;

	int temp_size = sizeof(SOURCE);
	ifstream fin(filename);
	if (!fin.is_open())
	{
		cerr << filename << "  open error  " << __FUNCTION__ << endl;
		return false;
	}
	fin.read((char*)&FileHead, sizeof(FILEHEAD));
	int pCount = FileHead.recnum;
	while (fin)
	{
		fin.read((char*)temp.data(), temp.size()*temp_size);
		int read_size = fin.gcount() / temp_size;
		if (read_size == 0)
		{
			break;
		}
		else
		{
			TARGET target;
			int target_size = sizeof(target);
			int index = 0;
			while (index != read_size)
			{
				memset(&target, 0, target_size);
				if (Trans(target,temp[index]))
				{
					_data[target.ukey].emplace_back(target);
				}
				index++;
			}
		}
	}
	return true;
}

template<typename SOURCE, typename TARGET>
inline bool Quote_Request<SOURCE, TARGET>::set_FileList(string & path, string & market, string & variety)
{
	DIR *	dir = nullptr;
	struct	dirent *ptr = nullptr;
	char	windCode[32]{ 0 };

	if ((dir = opendir(path.c_str())) == nullptr)
	{
		cerr << "BasePath is inexist" << endl;;
		return false;
	}
	while ((ptr = readdir(dir)) != nullptr)
	{
		if (strcmp(ptr->d_name, ".") == 0 || strcmp(ptr->d_name, "..") == 0)
		{
			continue;
		}
		else if (ptr->d_type == 8 && string(ptr->d_name).rfind(".bz2") != string::npos)
		{
			string filename = path;
			filename += "/";
			filename += ptr->d_name;
			auto pos_1 = filename.rfind('/') + 1;
			auto pos_2 = filename.rfind(".csv");
			memset(windCode, 0, sizeof(windCode));
			if (pos_1 == string::npos || pos_2 == string::npos)
			{
				continue;
			}
			strcpy(windCode, filename.substr(pos_1, pos_2 - pos_1).c_str());
			if (market == "SH" && variety == "index")
			{
				SH_IndexCodeTrans(windCode);
			}
			auto ukey = GetUkey(market, windCode);
			if (ukey == 0)
			{
				continue;
			}
			else
			{
				FileList[ukey] = filename;
			}
		}
		else if (ptr->d_type == 10)
		{
			continue;
		}
		else if (ptr->d_type == 4)
		{
			string Base;
			Base = path;
			Base += "/";
			Base += ptr->d_name;
			set_FileList(Base, market, variety);
		}
	}
	closedir(dir);
	return true;
}

template<typename SOURCE, typename TARGET>
inline bool Quote_Request<SOURCE, TARGET>::completion(function<void(int64_t,vector<string>&,list<TARGET>&)> Trans)
{
	if (_data.empty() && FileList.empty())
	{
		cerr << "no data to merge..." << endl;
		return false;
	}
	vector<string>    tdb_temp;
	list<TARGET>	  quote_temp;
	for (auto &it:FileList)
	{
		if (!DeCompress(it.second, tdb_temp))
		{
			cerr << "解压失败 : " << it.second << endl;
		}
		else
		{
			Trans(it.first, tdb_temp, quote_temp);
			auto is_find = _data.find(it.first);
			if (is_find == _data.end())//tdb独有,将tdb数据插入到_data中
			{
				_data[it.first] = quote_temp;
			}
			else//共有数据,将结果补全
			{
				list<TARGET> temp;
				auto it_begin = is_find->second.begin();
				auto it_end = is_find->second.end();
				for (;it_begin != it_end; )
				{
					if (it_begin->timeus <= quote_temp.front().timeus)//当内存块数据全,则取内存块数据
					{
						temp.emplace_back(*it_begin);
						it_begin++;
						continue;
					}
					else//当tdb数据全,取tdb数据,并将tdb的接收时间补全
					{
						temp.emplace_back(quote_temp.front());
						quote_temp.pop_front();
					}
				}
				_data[it.first].swap(temp);
			}
		}
	}
	tdb_temp.clear();
	quote_temp.clear();
}

template<typename SOURCE, typename TARGET>
inline bool Quote_Request<SOURCE, TARGET>::WriteToFile(function<void(TARGET*,string&)> Trans2str)
{
	string path = PATH + _date + "/";
	string filename = path;
	if (!mkdir_path(path))
	{
		cerr << "mkdir path = " << path << "  error!" << endl;
		return false;
	}
	if (typeid(TARGET).name() == typeid(Snapshot).name())
	{
		filename += "Snapshot";
	}
	else if(typeid(TARGET).name() == typeid(Order).name())
	{
		filename += "Order";
	}
	else if (typeid(TARGET).name() == typeid(OrderQueue).name())
	{
		filename += "OrderQueue";
	}
	else if (typeid(TARGET).name() == typeid(Transaction).name())
	{
		filename += "Transaction";
	}
	auto f1 = bind(WriteToCvs<TARGET>, std::ref(_data),std::ref(_date), Trans2str);
	auto f2 = bind(SortAndWrite<TARGET>, std::ref(_data), std::ref(filename),std::ref(_date));
	thread thread_tocsv(f1);
	thread thread_toquote(f2);
	thread_tocsv.join();
	thread_toquote.join();
	return true;
}

#endif