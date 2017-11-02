#include "cfg.h"

string date;
string type;
const string base_path = "/UKData/TDF/zhwb/";

template<typename SOUCE, typename TARGET>
bool process(string& filename, string& quote_name, Quote_Request<SOUCE, TARGET>& todata, function<bool(TARGET&, SOUCE&)> sds_to_quote, function<void(int64_t, vector<string>&, list<TARGET>&)> tdb_to_quote, function<void(TARGET*, string&)> quote_to_csv)
{
	cout << "开始读取 : " << filename << endl;
	auto ret = todata.set_data(filename, sds_to_quote);
	if (!ret)
	{
		cerr << "read  " << filename << " fail!" << endl;
		return false;
	}
	cout << "读取完成 : " << filename << endl;

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

void merge_Snapshot()
{
	set<string> markets;
	Init_ukdb09(markets, true);

	cout << "开始转换Snapshot_cfe数据..." << endl;
	Quote_Request<SDS20FUTURE, Snapshot>*  ToSnapshot_cfe = new Quote_Request<SDS20FUTURE, Snapshot>;
	ToSnapshot_cfe->set_date(date);
	auto stock_file_1 = [&ToSnapshot_cfe]() {
		string type = "tick";
		string market;
		string variety;
		string path;

		//future的tick:SHF CZC DCE
		variety = "future";
		market = "SHF";
		path = base_path + variety + "/" + type + "/" + market + "/" + date;
		ToSnapshot_cfe->set_FileList(path, market, variety);
		market = "CZC";
		path = base_path + variety + "/" + type + "/" + market + "/" + date;
		ToSnapshot_cfe->set_FileList(path, market, variety);
		market = "DCE";
		path = base_path + variety + "/" + type + "/" + market + "/" + date;
		ToSnapshot_cfe->set_FileList(path, market, variety);
	};
	stock_file_1();
	string filename = "/UKData/TDF/sds/" + date;
	filename = FindFileName<Type::CFE>(filename);
	if (filename.empty())
	{
		return;
	}
	else
	{
		string quote_file_name = "Snapshot_cfe";
		if (!process<SDS20FUTURE, Snapshot>(filename, quote_file_name, *ToSnapshot_cfe, Cfe_SpiToSnapshot, TdbToSnapshot, Snapshot2str))
		{
			return;
		}
	}
	delete ToSnapshot_cfe;

	cout << "开始转换Snapshot_idx数据..." << endl;
	Quote_Request<SDS20INDEX, Snapshot>*  ToSnapshot_idx = new Quote_Request<SDS20INDEX, Snapshot>;
	ToSnapshot_idx->set_date(date);
	auto stock_file_2 = [&ToSnapshot_idx]() {
		string type = "tick";
		string market;
		string variety;
		string path;

		//index的tick:SZ SH
		variety = "index";
		market = "SZ";
		path = base_path + variety + "/" + type + "/" + market + "/" + date;
		ToSnapshot_idx->set_FileList(path, market, variety);
		market = "SH";
		path = base_path + variety + "/" + type + "/" + market + "/" + date;
		ToSnapshot_idx->set_FileList(path, market, variety);
	};
	stock_file_2();
	filename = "/UKData/TDF/sds/" + date;
	filename = FindFileName<Type::IDX>(filename);
	if (filename.empty())
	{
		return;
	}
	else
	{
		string quote_file_name = "Snapshot_idx";
		if (!process<SDS20INDEX, Snapshot>(filename, quote_file_name, *ToSnapshot_idx, IdxToSnapshot, TdbToSnapshot, Snapshot2str))
		{
			return;
		}
	}
	delete ToSnapshot_idx;

	cout << "开始转换Snapshot_spi数据..." << endl;
	Quote_Request<SDS20FUTURE, Snapshot>*  ToSnapshot_spi = new Quote_Request<SDS20FUTURE, Snapshot>;
	ToSnapshot_spi->set_date(date);
	auto stock_file_3 = [&ToSnapshot_spi]() {
		string type = "tick";
		string market = "CF";
		string variety;
		string path;

		//index的tick:SZ SH
		variety = "future";
		path = base_path + variety + "/" + type + "/" + market + "/" + date;
		ToSnapshot_spi->set_FileList(path, market, variety);
	};
	stock_file_3();
	filename = "/UKData/TDF/sds/" + date;
	filename = FindFileName<Type::SPI>(filename);
	if (filename.empty())
	{
		return;
	}
	else
	{
		string quote_file_name = "Snapshot_spi";
		if (!process<SDS20FUTURE, Snapshot>(filename, quote_file_name, *ToSnapshot_spi, Cfe_SpiToSnapshot, TdbToSnapshot, Snapshot2str))
		{
			return;
		}
	}
	delete ToSnapshot_spi;
}

void merge_Order()
{
	set<string> markets;
	Init_ukdb09(markets,true);
	cout << "开始转换Order数据..." << endl;

	Quote_Request<SDS20ORDER, Order>*  ToOrder = new Quote_Request<SDS20ORDER, Order>;
	ToOrder->set_date(date);

	auto stock_file = [&ToOrder]() {
		string type = "order";
		string market = "SZ";
		string variety;
		string path;

		//stock的order:SZ 
		variety = "stock";
		path = base_path + variety + "/" + type + "/" + market + "/" + date;
		ToOrder->set_FileList(path, market, variety);
		//bond的order:SZ
		variety = "bond";
		path = base_path + variety + "/" + type + "/" + market + "/" + date;
		ToOrder->set_FileList(path, market, variety);
		//fund的order:SZ
		variety = "fund";
		path = base_path + variety + "/" + type + "/" + market + "/" + date;
		ToOrder->set_FileList(path, market, variety);
	};
	stock_file();

	string filename = "/UKData/TDF/sds/"+ date;
	filename = FindFileName<Type::ORD>(filename);
	if (filename.empty())
	{
		return;
	}
	else
	{
		string quote_file_name = "Order";
		if (!process<SDS20ORDER,Order>(filename,quote_file_name,*ToOrder, OrdToOrder,TdbToOrder,Order2str))
		{
			return;
		}
	}
	delete ToOrder;
}

void merge_OrderQueue()
{
	set<string> markets;
	Init_ukdb09(markets, true);
	cout << "开始转换OrderQueue数据..." << endl;

	Quote_Request<SDS20ORDERQUEUE, OrderQueue>*  ToOrderQueue = new Quote_Request<SDS20ORDERQUEUE, OrderQueue>;
	ToOrderQueue->set_date(date);

	auto stock_file = [&ToOrderQueue]() {
		string type = "orderqueue";
		string market;
		string variety;
		string path;

		//stock的orderqueue:SZ SH
		variety = "stock";
		market = "SZ";
		path = base_path + variety + "/" + type + "/" + market + "/" + date;
		ToOrderQueue->set_FileList(path, market, variety);
		market = "SH";
		path = base_path + variety + "/" + type + "/" + market + "/" + date;
		ToOrderQueue->set_FileList(path, market, variety);

		//bond的orderqueue:SZ SH
		variety = "bond";
		market = "SZ";
		path = base_path + variety + "/" + type + "/" + market + "/" + date;
		ToOrderQueue->set_FileList(path, market, variety);
		market = "SH";
		path = base_path + variety + "/" + type + "/" + market + "/" + date;
		ToOrderQueue->set_FileList(path, market, variety);

		//fund的orderqueue:SZ SH
		variety = "fund";
		market = "SZ";
		path = base_path + variety + "/" + type + "/" + market + "/" + date;
		ToOrderQueue->set_FileList(path, market, variety);
		market = "SH";
		path = base_path + variety + "/" + type + "/" + market + "/" + date;
		ToOrderQueue->set_FileList(path, market, variety);
	};
	stock_file();

	string filename = "/UKData/TDF/sds/" + date;
	filename = FindFileName<Type::ORQ>(filename);
	if (filename.empty())
	{
		return;
	}
	else
	{
		string quote_file_name = "OrderQueue";
		if (!process<SDS20ORDERQUEUE, OrderQueue>(filename, quote_file_name, *ToOrderQueue, OrqToOrderqueue, TdbToOrderQueue, Orderque2str))
		{
			return;
		}
	}
	delete ToOrderQueue;
}

void merge_Transaction()
{
	set<string> markets;
	Init_ukdb09(markets, true);
	cout << "开始转换Transaction数据..." << endl;

	Quote_Request<SDS20TRANSACTION, Transaction>*  ToTransaction = new Quote_Request<SDS20TRANSACTION, Transaction>;
	ToTransaction->set_date(date);

	auto stock_file = [&ToTransaction]() {
		string type = "trans";
		string market;
		string variety;
		string path;

		//stock的trans:SZ SH
		variety = "stock";
		market = "SZ";
		path = base_path + variety + "/" + type + "/" + market + "/" + date;
		ToTransaction->set_FileList(path, market, variety);
		market = "SH";
		path = base_path + variety + "/" + type + "/" + market + "/" + date;
		ToTransaction->set_FileList(path, market, variety);

		//bond的trans:SZ SH
		variety = "bond";
		market = "SZ";
		path = base_path + variety + "/" + type + "/" + market + "/" + date;
		ToTransaction->set_FileList(path, market, variety);
		market = "SH";
		path = base_path + variety + "/" + type + "/" + market + "/" + date;
		ToTransaction->set_FileList(path, market, variety);

		//fund的trans:SZ SH
		variety = "fund";
		market = "SZ";
		path = base_path + variety + "/" + type + "/" + market + "/" + date;
		ToTransaction->set_FileList(path, market, variety);
		market = "SH";
		path = base_path + variety + "/" + type + "/" + market + "/" + date;
		ToTransaction->set_FileList(path, market, variety);
	};
	stock_file();

	string filename = "/UKData/TDF/sds/" + date;
	filename = FindFileName<Type::TRD>(filename);
	if (filename.empty())
	{
		return;
	}
	else
	{
		string quote_file_name = "Transaction";
		if (!process<SDS20TRANSACTION, Transaction>(filename, quote_file_name, *ToTransaction, TrdToTransaction, TdbToTransaction, Trans2str))
		{
			return;
		}
	}
	delete ToTransaction;
}

void MySort(list<Snapshot> &v_data, string FileName)
{
	vector<Snapshot*>v_ptr;
	v_ptr.reserve(v_data.size());
	for (auto &it : v_data)
	{
		v_ptr.push_back(&it);
	}
	sort(v_ptr.begin(), v_ptr.end(), compare_quote<Snapshot>());
	ofstream fout(FileName, ios_base::app);
	if (!fout.is_open())
		return;
	for (auto &it : v_ptr)
	{
		fout.write((const char *)it, sizeof(Snapshot));
	}
	fout.close();
}

bool merger_sort(map<string, int64_t> FileList, int64_t EndTime, string FileName)
{
	if (FileList.empty()) {
		cout << "merger_sort is seccuss!" << endl;
		return true;
	}

	ifstream file;
	vector<string>		MyErase;
	list<Snapshot>		v_snapshot;
	Snapshot tep = { 0 };
	int length = 0;
	int tep_size = sizeof(tep);

	for (auto &it : FileList)
	{
		file.open(it.first);
		if (!file.is_open())
		{
			cerr << "open " << it.first << " is error" << endl;
			return false;
		}
		FILEHEAD FileHead = { 0 };
		file.read((char*)&FileHead, sizeof(FileHead));
		length = FileHead.recnum;
		if (length <= it.second)
		{
			MyErase.push_back(it.first);
			file.close();
			continue;
		}
		file.seekg(sizeof(FileHead) + tep_size*it.second, ios_base::beg);
		while (file)
		{
			memset(&tep, 0, tep_size);
			file.read((char*)&tep, sizeof(tep));
			if (file.gcount() != tep_size)
				break;
			if (tep.timeus < EndTime)
			{
				v_snapshot.emplace_back(tep);
				it.second++;
			}
			else {
				file.seekg(-tep_size, ios_base::cur);
				break;
			}
		}
		file.close();
	}

	for (auto it : MyErase)
	{
		FileList.erase(it);
	}
	MySort(v_snapshot, FileName);
	v_snapshot.clear();
	MyErase.clear();
	merger_sort(FileList, EndTime + 7200000000, FileName);
	return true;
}

void snapshot_sort()
{
	string Path = PATH + date + "/";
	string Filesnap_0 = Path + "Snapshot_lvt_SZ";
	string Filesnap_1 = Path + "Snapshot_lvt_SH";
	string Filesnap_2 = Path + "Snapshot_cfe";
	string Filesnap_3 = Path + "Snapshot_idx";
	string Filesnap_4 = Path + "Snapshot_spi";
	string FileSouce = Path + date + "_Snapshot";

	map<string, int64_t> FileList{
		{ Filesnap_0, 0 },
		{ Filesnap_1, 0 },
		{ Filesnap_2, 0 },
		{ Filesnap_3, 0 },
		{ Filesnap_4, 0 },
	};

	ofstream fou(FileSouce);
	if (!fou.is_open())
	{
		cerr << "open FileSouce is error" << endl;
		return;
	}

	ifstream fin;
	FILEHEAD FileHead_s = { 0 };
	FILEHEAD FileHead = { 0 };
	Snapshot temp = { 0 };
	int64_t MinTime = GetMsTime(21000110, 80000000);
	for (auto &it : FileList)
	{
		fin.open(it.first);
		if (!fin.is_open())
		{
			cerr << "open FileList is error" << endl;
			return;
		}
		memset((void*)&FileHead, 0, sizeof(FileHead));
		memset((void*)&temp, 0, sizeof(temp));
		fin.read((char*)&FileHead, sizeof(FileHead));
		fin.read((char*)&temp, sizeof(temp));
		MinTime = (temp.timeus < MinTime ? temp.timeus : MinTime);
		FileHead_s.recnum += FileHead.recnum;
		fin.close();
	}
	FileHead_s.bytes_per_record = FileHead.bytes_per_record;
	FileHead_s.data_shmid = FileHead.data_shmid;
	strcpy(FileHead_s.flags, FileHead.flags);
	strcpy(FileHead_s.maintype, FileHead.maintype);
	FileHead_s.main_shmid = FileHead.main_shmid;
	FileHead_s.recnum_per_block = FileHead.recnum_per_block;
	FileHead_s.updatetm = FileHead.updatetm;
	FileHead_s.version = FileHead.version;
	fou.write((const char*)&FileHead_s, sizeof(FileHead_s));
	fou.close();

	int64_t stepTime = 2 * 3600;//7200000000
	int64_t EndTime = MinTime + stepTime * 1000000;
	merger_sort(FileList, EndTime, FileSouce);

	for (auto &it : FileList)
	{
		if (remove(it.first.c_str()))
		{
			cerr << "remove " << Filesnap_0 << " is error" << endl;
		}
	}
}


const int max_len = 1024 * 1024 * 10;
char *content = nullptr;
int main(int argc,char* argv[])
{
	if (argc < 3)
	{
		cerr << "usr:  date type" << endl;
		return -1;
	}
	date = argv[1];
	string type = argv[2];
	content = new char[max_len];
	map<string, function<void()>> func{
		{ "TICK", merge_Snapshot },
		{ "ORDER", merge_Order },
		{ "ORDERQUEUE", merge_OrderQueue },
		{ "TRANS", merge_Transaction },
		{ "SORT", snapshot_sort },
	};
	func[type]();
	delete [] content;
    return 0;
}