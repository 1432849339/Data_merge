#ifndef __TRANS_H__
#define __TRANS_H__
#include "cfg.h"

template<typename T>
bool WriteToCvs(map<int64_t, list<shared_ptr<T>>>& ukey_list_data, string& date, function<void(T*, string&)> Trans2str)
{
	extern map<int, string> MarketID;
	extern map<int, string> VarID;
#ifndef TOREQUEUEST
	const string base_path = "/UKData/TDF/fqy/Request.csv/";
#else
	const string base_path = "/UKData/TDF/fqy/" + date + "/";
#endif
	
	if (ukey_list_data.empty())
	{
		return false;
	}
	int pCount = static_cast<int>(ukey_list_data.size());
	int index = 0;
	for (auto &data : ukey_list_data)
	{
		index++;
		string	request_name_csv = base_path;
		int64_t ukey = data.first;
		int mk = 0, ty = 0;
		get_variety_market_by_ukey(ukey, ty, mk);
		if ((MarketID.find(mk) != MarketID.end()) && (VarID.find(ty) != VarID.end()))
		{
			map<string, string>Request_FileName
			{
				{ "N7chronos10OrderQueueE", "OrderQueue.csv" },
				{ "N7chronos11TransactionE", "Transaction.csv" },
				{ "N7chronos5OrderE", "Order.csv" },
				{ "N7chronos8SnapshotE", "Snapshot.csv" },
			};
			request_name_csv += MarketID[mk];
			request_name_csv += "/";
			request_name_csv += VarID[ty];
			request_name_csv += "/";
			request_name_csv += itostr(ukey);
			request_name_csv += "/";
			request_name_csv += date.substr(0, 4);
			request_name_csv += "/";
			request_name_csv += date;
			request_name_csv += "/";
			while (!mkdir_path(request_name_csv))
			{
				cout << "waitting mkdir " << request_name_csv << endl;
			}
			request_name_csv += Request_FileName[typeid(T).name()];
			ofstream fout_csv(request_name_csv);
			if (!fout_csv.is_open())
			{
				cerr << "create " << request_name_csv << "  error" << endl;
				return false;
			}
			string str;
			for (auto &it : data.second)
			{
				str.clear();
				Trans2str(it.get(), str);
				fout_csv << str;
			}
			fout_csv.close();
		}
		printf("%.2lf%%   位置:%d   总数:%d  \r", index * 100.0 / pCount, index, pCount);
		fflush(stdout);
	}
	return true;
}
//写入回放数据
template<typename T>
bool SortAndWrite(map<int64_t, list<shared_ptr<T>>>& ukey_list_data, string& FileName, string& date)
{
	vector<T*> v_ptr;
	map<string, string>Request_FileName
	{
		{ "N7chronos10OrderQueueE", "OrderQueue" },
		{ "N7chronos11TransactionE", "Transaction" },
		{ "N7chronos5OrderE", "Order" },
		{ "N7chronos8SnapshotE", "Snapshot" },
	};
	v_ptr.reserve(20000000);
	for (auto &it : ukey_list_data)
	{
		for (auto &te : it.second)
		{
			v_ptr.emplace_back(te.get());
		}
	}
	std::sort(v_ptr.begin(), v_ptr.end(), compare_quote<T>());

	ofstream fout(FileName);
	if (!fout.is_open())
	{
		cerr << "create " << FileName << " error" << endl;
		return false;
	}
	FILEHEAD FileHead = { 0 };
	FileHead.bytes_per_record = sizeof(T);
	FileHead.recnum = static_cast<int>(v_ptr.size());
	FileHead.updatetm = static_cast<int>(time(NULL));
	if (Request_FileName[typeid(T).name()] == "OrderQueue")
	{
		strcpy(FileHead.maintype, ("ORQ" + date).c_str());
	}
	else if (Request_FileName[typeid(T).name()] == "Transaction") {
		strcpy(FileHead.maintype, ("TRD" + date).c_str());
	}
	else if (Request_FileName[typeid(T).name()] == "Order") {
		strcpy(FileHead.maintype, ("ORD" + date).c_str());
	}
	else if (Request_FileName[typeid(T).name()] == "Snapshot") {
		strcpy(FileHead.maintype, ("LVT" + date).c_str());
	}
	fout.write((const char*)&FileHead, sizeof(FileHead));
	for (auto &it : v_ptr)
	{
		fout.write((const char *)it, sizeof(T));
	}
	fout.close();
	return true;
}

template<typename SOURCE, typename TARGET>
class Quote_Request {
public:
	//通用结构
	Quote_Request()
	{
		_market.clear();
		_spit_by_market = false;
	}
	Quote_Request(Quote_Request& ref) = delete;
	void set_date(const string& date);
	void set_market(const string& market);
	void set_filename(string& filename);
	bool set_data(string& filename, function<bool(TARGET&, SOURCE&)> Trans);
	bool set_FileList(string& path, string& market, string& variety);
	bool completion(function<void(int64_t, vector<string>&, list<shared_ptr<TARGET>>&)> Trans);
	bool WriteToFile(function<void(TARGET*, string&)> Trans2str);
	bool is_include_orderque();
	void join_orderqueue();
	void show_filelist();
	//lvt特有,补全orderqueue字段
protected:
	bool make_dick(map<int64_t, map<int32_t, map<int64_t, shared_ptr<OrderQueue>>>>& dick);

private:
	bool                             _spit_by_market;
	string							 _market;
	string							 _filename;
	string							 _date;
	map<int64_t, string>			  FileList;
	map<int64_t, list<shared_ptr<TARGET>>>		 _data;
};

template<typename SOURCE, typename TARGET>
inline void Quote_Request<SOURCE, TARGET>::set_date(const string & date)
{
	_date = date;
}

template<typename SOURCE, typename TARGET>
inline void Quote_Request<SOURCE, TARGET>::set_market(const string & market)
{
	_market = market;
	_spit_by_market = true;
}

template<typename SOURCE, typename TARGET>
inline void Quote_Request<SOURCE, TARGET>::set_filename(string & filename)
{
	_filename = filename;
}

template<typename SOURCE, typename TARGET>
inline bool Quote_Request<SOURCE, TARGET>::set_data(string & filename, function<bool(TARGET&, SOURCE&)> Trans)
{
	FILEHEAD				FileHead{ 0 };
	array<SOURCE, 10000>	temp;
	auto temp_size = sizeof(SOURCE);
	ifstream fin(filename);
	if (!fin.is_open())
	{
		cerr << filename << "  open error  " << __FUNCTION__ << endl;
		return false;
	}

	assert(temp.size() == 10000);
	fin.read((char*)&FileHead, sizeof(FILEHEAD));
	int remainder = FileHead.recnum % static_cast<int>(temp.size());
	int pCount = FileHead.recnum / static_cast<int>(temp.size());
	pCount = (remainder == 0 ? pCount : (pCount + 1));
	int index = 0;
#ifdef DEBUG
	int count = 0;
#endif
	while (fin)
	{
#ifdef DEBUG
		if (count++ == 20)
		{
			cout << _date.size() << endl;
			break;
		}
#endif
		index++;
		fin.read((char*)temp.data(), temp.size()*temp_size);
		int read_size = static_cast<int>(fin.gcount() / static_cast<streamsize>(temp_size));
		if (read_size == 0)
		{
			printf("%.2lf%%   位置:%d   总数:%d  \r", index * 100.0 / pCount, index, pCount);
			fflush(stdout);
			break;
		}
		else
		{
			int target_size = sizeof(TARGET);
			int index = 0;
			while (index != read_size)
			{
				if (_spit_by_market)
				{
					string windcode = string(temp[index].szWindCode);
					if (strcasecmp(_market.c_str(), windcode.substr(windcode.rfind('.') + 1).c_str()) == 0)
					{
						shared_ptr<TARGET> target(new TARGET);
						memset(target.get(), 0, target_size);
						if (Trans(*target, temp[index]))
						{
							_data[target->ukey].emplace_back(target);
						}
					}
				}
				else
				{
					shared_ptr<TARGET> target(new TARGET);
					memset(target.get(), 0, target_size);
					if (Trans(*target, temp[index]))
					{
						_data[target->ukey].emplace_back(target);
					}
				}
				index++;
			}
		}
		printf("%.2lf%%   位置:%d   总数:%d  \r", index * 100.0 / pCount, index, pCount);
		fflush(stdout);
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
		cerr << "no  :"<< path << endl;;
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
inline bool Quote_Request<SOURCE, TARGET>::completion(function<void(int64_t, vector<string>&, list<shared_ptr<TARGET>>&)> Trans)
{
	if (_data.empty() && FileList.empty())
	{
		cerr << "no data to merge..." << endl;
		return false;
	}
	vector<string>				  tdb_temp;
	list<shared_ptr<TARGET>>	  quote_temp;

	int	 index = 0;
	auto file_count = FileList.size();
#ifdef DEBUG
	int count = 0;
	cout << FileList.size() << endl;
#endif
	for (auto &it : FileList)
	{
#ifdef DEBUG
		if (count++ == 1)
		{
			break;
		}
#endif // DEBUG
		index++;
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
				list<shared_ptr<TARGET>>	temp;
				auto it_begin = is_find->second.begin();
				auto it_end = is_find->second.end();
				int64_t last_recvus = quote_temp.front()->recvus;
// 				cout << "quote_temp.size()" <<quote_temp.size() << endl;
// 				cout << "is_find->second.size()" <<is_find->second.size() << endl;
				for (; it_begin != it_end; )
				{
					if (quote_temp.empty())//tdb数据没有了,则顺序将sds数据写入
					{
						temp.emplace_back(*it_begin);
						it_begin++;
					}
					else
					{
						if ((*it_begin)->timeus < quote_temp.front()->timeus)//当内存块数据全,则取内存块数据
						{
							temp.emplace_back(*it_begin);
							last_recvus = (*it_begin)->recvus;
							it_begin++;
						}
						else if ((*it_begin)->timeus == quote_temp.front()->timeus)//相等时取sds
						{
							temp.emplace_back(*it_begin);
							last_recvus = (*it_begin)->recvus;
							it_begin++;
							quote_temp.pop_front();
						}
						else//当tdb数据全,取tdb数据,并将tdb的接收时间补全
						{
							if (quote_temp.front()->recvus < last_recvus)
							{
								quote_temp.front()->recvus = last_recvus;
							}
							else
							{
								last_recvus = quote_temp.front()->recvus;
							}
							temp.emplace_back(quote_temp.front());
							quote_temp.pop_front();
						}
					}
				}
				//cout << "quote_temp.size()" <<quote_temp.size() << endl;
				if (!quote_temp.empty())//tdb数据没完
				{
					for (auto t = quote_temp.begin(); t != quote_temp.end(); ++t)
					{
						if ((*t)->recvus >= last_recvus)
						{
							temp.insert(temp.end(), t, quote_temp.end());
							break;
						}
						else
						{
							(*t)->recvus = last_recvus;
							temp.emplace_back(*t);
						}
					}
				}
				//cout << "temp.size() = " <<temp.size() << endl;
				_data[it.first].swap(temp);
			}
		}
		printf("%.2lf%%   位置:%d   总数:%d  \r", index * 100.0 / file_count, index, file_count);
		fflush(stdout);
		tdb_temp.clear();
		quote_temp.clear();
	}
	return true;
}

template<typename SOURCE, typename TARGET>
inline bool Quote_Request<SOURCE, TARGET>::WriteToFile(function<void(TARGET*, string&)> Trans2str)
{
	string path = PATH + _date + "/";
	string filename = path;
	if (!mkdir_path(path))
	{
		cerr << "mkdir path = " << path << "  error!" << endl;
		return false;
	}
	filename += _date + "_" + _filename;
#ifndef TOREQUEUEST
	auto f1 = bind(WriteToCvs<TARGET>, std::ref(_data), std::ref(_date), Trans2str);
	auto f2 = bind(SortAndWrite<TARGET>, std::ref(_data), std::ref(filename), std::ref(_date));
	thread thread_tocsv(f1);
	thread thread_toquote(f2);
	thread_tocsv.join();
	thread_toquote.join();
#else
	auto f1 = bind(WriteToCvs<TARGET>, std::ref(_data), std::ref(_date), Trans2str);
	thread thread_tocsv(f1);
	thread_tocsv.join();
#endif
	return true;
}

template<typename SOURCE, typename TARGET>
inline bool Quote_Request<SOURCE, TARGET>::is_include_orderque()
{
	return _spit_by_market;
}

template<typename SOURCE, typename TARGET>
inline void Quote_Request<SOURCE, TARGET>::join_orderqueue()
{
	map<int64_t, map<int32_t, map<int64_t, shared_ptr<OrderQueue>>>>   dick;
	if (!make_dick(dick))
	{
		cerr << "make_dick error" << endl;
		return;
	}
	int   index = 0;
	auto  pcount = _data.size();
	for (auto &it : _data)
	{
		index++;
		auto is_find_ukey = dick.find(it.first);
		if (is_find_ukey != dick.end())
		{
			bool has_ask_que = false;
			int ask_que = static_cast<int>('A');
			auto is_find_ask = is_find_ukey->second.find(ask_que);
			if (is_find_ask != is_find_ukey->second.end())
			{
				has_ask_que = true;
			}
			bool has_bid_que = false;
			int bid_que = static_cast<int>('B');
			auto is_find_bid = is_find_ukey->second.find(bid_que);
			if (is_find_ask != is_find_ukey->second.end())
			{
				has_bid_que = true;
			}
			for (auto &lvt : it.second)
			{
				int64_t  an = 0;
				int64_t* aa = NULL;
				if (has_ask_que)
				{
					auto que = is_find_ask->second.find(lvt.get()->timeus);
					if (que != is_find_ask->second.end())
					{
						auto poq = que->second;
						an = poq->orders_num;
						aa = poq->queue;
					}
				}
				int64_t  bn = 0;
				int64_t* bb = NULL;
				if (has_bid_que)
				{
					auto que = is_find_bid->second.find(lvt.get()->timeus);
					if (que != is_find_bid->second.end())
					{
						auto poq = que->second;
						bn = poq->orders_num;
						bb = poq->queue;
					}
				}
				lvt.get()->ask_orders_num = an;
				lvt.get()->bid_orders_num = bn;
				if (aa != NULL)
				{
					for (int i = 0; i < 50; ++i)
					{
						lvt.get()->ask_queue[i] = aa[i];
					}
				}
				if (bb != NULL)
				{
					for (int i = 0; i < 50; ++i)
					{
						lvt.get()->bid_queue[i] = bb[i];
					}
				}
			}
		}
		printf("%.2lf%%   位置:%d   总数:%d  \r", index * 100.0 / pcount, index, pcount);
		fflush(stdout);
	}
}

template<typename SOURCE, typename TARGET>
inline void Quote_Request<SOURCE, TARGET>::show_filelist()
{
	int i = 0;
	for (auto &it : FileList)
	{
		cout << it.first << "\t" << it.second << "\t" << i++ << endl;
	}
	cout << "文件个数: " << FileList.size() << endl;
}

template<typename SOURCE, typename TARGET>
inline bool Quote_Request<SOURCE, TARGET>::make_dick(map<int64_t, map<int32_t, map<int64_t, shared_ptr<OrderQueue>>>>& dick)
{
	if (!_spit_by_market)
	{
		return false;
	}
	FILEHEAD					FileHead{ 0 };
	array<OrderQueue, 10000>	temp;
	auto temp_size = sizeof(OrderQueue);
	string filename = PATH + _date + "/";
	filename += _date + "_OrderQueue";
	ifstream fin(filename);
	if (!fin.is_open())
	{
		cerr << filename << "  open error  " << __FUNCTION__ << endl;
		return false;
	}
	assert(temp.size() == 10000);
	fin.read((char*)&FileHead, sizeof(FILEHEAD));
	int remainder = FileHead.recnum % static_cast<int>(temp.size());
	int pCount = FileHead.recnum / static_cast<int>(temp.size());
	pCount = (remainder == 0 ? pCount : (pCount + 1));
	int			index = 0;
	map<int, string>  market_type
	{
		{ MARKET_SZA,"SZ" },
		{ MARKET_SZB,"SZ" },
		{ MARKET_SHA,"SH" },
		{ MARKET_SHB,"SH" },
	};
	while (fin)
	{
		index++;
		fin.read((char*)temp.data(), temp.size()*temp_size);
		int read_size = static_cast<int>(fin.gcount() / static_cast<streamsize>(temp_size));
		if (read_size == 0)
		{
			printf("%.2lf%%   位置:%d   总数:%d  \r", index * 100.0 / pCount, index, pCount);
			fflush(stdout);
			break;
		}
		else
		{
			int index = 0;
			while (index != read_size)
			{
				int variety = 0;
				int market = 0;
				get_variety_market_by_ukey(temp[index].ukey, variety, market);
				if (strcasecmp(_market.c_str(), market_type[market].c_str()) == 0)
				{
					shared_ptr<OrderQueue> target(new OrderQueue);
					memcpy(target.get(), &temp[index], temp_size);
					dick[target->ukey][target->side][target->timeus] = target;
				}
				index++;
			}
		}
		printf("%.2lf%%   位置:%d   总数:%d  \r", index * 100.0 / pCount, index, pCount);
		fflush(stdout);
	}
	return true;
}


/**********************************************************************************/
template<typename SOUCE, typename TARGET>
bool process(string& filename, string& quote_name, Quote_Request<SOUCE, TARGET>& todata, function<bool(TARGET&, SOUCE&)> sds_to_quote, function<void(int64_t, vector<string>&, list<shared_ptr<TARGET>>&)> tdb_to_quote, function<void(TARGET*, string&)> quote_to_csv)
{
	int ret = 0;
#ifndef COMPLETION
	cout << "历史数据生成" << endl;
#else
	cout << "开始读取 : " << filename << endl;
	ret = todata.set_data(filename, sds_to_quote);
	if (!ret)
	{
		cerr << "read  " << filename << " fail!" << endl;
		return false;
	}
	cout << "读取完成 : " << filename << endl;
#endif // !COMPLETION
	cout << "开始补全 : " << filename << endl;
	ret = todata.completion(tdb_to_quote);
	if (!ret)
	{
		cerr << "补全 : " << filename << " fail!" << endl;
		return false;
	}
	cout << "补全完成 : " << filename << endl;

	cout << "开始写入 : " << filename << endl;
	todata.set_filename(quote_name);
	ret = todata.WriteToFile(quote_to_csv);
	if (!ret)
	{
		cerr << "写入文件 : " << filename << " fail!" << endl;
		return false;
	}
	cout << "写入完成 : " << filename << endl;
	return true;
}

template<>
bool process<SDS20LEVEL2, Snapshot>(string& filename, string& quote_name, Quote_Request<SDS20LEVEL2, Snapshot>& todata, function<bool(Snapshot&, SDS20LEVEL2&)> sds_to_quote, function<void(int64_t, vector<string>&, list<shared_ptr<Snapshot>>&)> tdb_to_quote, function<void(Snapshot*, string&)> quote_to_csv)
{
	int ret = 0;
#ifndef COMPLETION
	cout << "历史数据生成" << endl;
#else
	cout << "开始读取 : " << filename << endl;
	ret = todata.set_data(filename, sds_to_quote);
	if (!ret)
	{
		cerr << "read  " << filename << " fail!" << endl;
		return false;
	}
	cout << "读取完成 : " << filename << endl;
#endif // !COMPLETION
	cout << "开始补全 : " << filename << endl;
	ret = todata.completion(tdb_to_quote);
	if (!ret)
	{
		cerr << "补全 : " << filename << " fail!" << endl;
		return false;
	}
	cout << "补全完成 : " << filename << endl;
#ifndef TOREQUEUEST
	if (todata.is_include_orderque())
	{
		cout << "开始加入orderque:" << endl;
		todata.join_orderqueue();
		cout << "加入orderque完成:" << endl;
	}
#endif
	cout << "开始写入 : " << filename << endl;
	todata.set_filename(quote_name);
	ret = todata.WriteToFile(quote_to_csv);
	if (!ret)
	{
		cerr << "写入文件 : " << filename << " fail!" << endl;
		return false;
	}
	cout << "写入完成 : " << filename << endl;
	return true;
}

#endif // !__TRANS_H__