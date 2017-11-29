#include <iostream>
#include "cfg.h"
#include "Trans.hpp"

string date;
string type;
const string base_path = "/UKData/TDF/zhwb/";
const int max_len = 1024 * 1024 * 10;
char *content = nullptr;

void merge_snapshot()
{
	set<string> markets;
#ifdef DEBUG
	Init_ukdb09(markets, true);
#else
	Init_ukdb09(markets);
#endif // DEBUG
	auto Trans_snapshot_cfe = [&]() {
		cout << "开始转换Snapshot_cfe数据..." << endl;
		Quote_Request<SDS20FUTURE, Snapshot>*  ToSnapshot_cfe = new Quote_Request<SDS20FUTURE, Snapshot>;
		ToSnapshot_cfe->set_date(date);
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
		string filename = "/UKData/TDF/sds/" + date;
#ifdef COMPLETION
		filename = FindFileName<Type::CFE>(filename);
		if (filename.empty())
		{
			return;
		}
#endif // COMPLETION
		string quote_file_name = "Snapshot_cfe";
		if (!process<SDS20FUTURE, Snapshot>(filename, quote_file_name, *ToSnapshot_cfe, Cfe_SpiToSnapshot, TdbToSnapshot, Snapshot2str))
		{
			return;
		}
		delete ToSnapshot_cfe;
	};
	Trans_snapshot_cfe();

	auto Trans_snapshot_idx = [&]() {
		cout << "开始转换Snapshot_idx数据..." << endl;
		Quote_Request<SDS20INDEX, Snapshot>*  ToSnapshot_idx = new Quote_Request<SDS20INDEX, Snapshot>;
		ToSnapshot_idx->set_date(date);

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
		string filename = "/UKData/TDF/sds/" + date;
#ifdef COMPLETION
		filename = FindFileName<Type::IDX>(filename);
		if (filename.empty())
		{
			return;
		}
#endif // COMPLETION
		string quote_file_name = "Snapshot_idx";
		if (!process<SDS20INDEX, Snapshot>(filename, quote_file_name, *ToSnapshot_idx, IdxToSnapshot, TdbToSnapshot, Snapshot2str))
		{
			return;
		}
		delete ToSnapshot_idx;
	};
	Trans_snapshot_idx();

	auto Trans_snapshot_spi = [&]() {
		cout << "开始转换Snapshot_spi数据..." << endl;
		Quote_Request<SDS20FUTURE, Snapshot>*  ToSnapshot_spi = new Quote_Request<SDS20FUTURE, Snapshot>;
		ToSnapshot_spi->set_date(date);

		string type = "tick";
		string market = "CF";
		string variety;
		string path;

		variety = "future";
		path = base_path + variety + "/" + type + "/" + market + "/" + date;
		ToSnapshot_spi->set_FileList(path, market, variety);
		string filename = "/UKData/TDF/sds/" + date;
#ifdef COMPLETION
		filename = FindFileName<Type::SPI>(filename);
		if (filename.empty())
		{
			return;
		}
#endif // COMPLETION
		string quote_file_name = "Snapshot_spi";
		if (!process<SDS20FUTURE, Snapshot>(filename, quote_file_name, *ToSnapshot_spi, Cfe_SpiToSnapshot, TdbToSnapshot, Snapshot2str))
		{
			return;
		}
		delete ToSnapshot_spi;
	};
	Trans_snapshot_spi();

	auto Trans_snapshot_lvt_sz = [&]() {
		cout << "开始转换Snapshot_lvt_sz数据..." << endl;
		Quote_Request<SDS20LEVEL2, Snapshot>*  Snapshot_lvt_sz = new Quote_Request<SDS20LEVEL2, Snapshot>;
		Snapshot_lvt_sz->set_date(date);

		string type = "tick";
		string market = "SZ";
		string variety;
		string path;
		Snapshot_lvt_sz->set_market(market);
		variety = "stock";
		path = base_path + variety + "/" + type + "/" + market + "/" + date;
		Snapshot_lvt_sz->set_FileList(path, market, variety);
		variety = "bond";
		path = base_path + variety + "/" + type + "/" + market + "/" + date;
		Snapshot_lvt_sz->set_FileList(path, market, variety);
		variety = "fund";
		path = base_path + variety + "/" + type + "/" + market + "/" + date;
		Snapshot_lvt_sz->set_FileList(path, market, variety);
		string filename = "/UKData/TDF/sds/" + date;
#ifdef COMPLETION
		filename = FindFileName<Type::LVT>(filename);
		if (filename.empty())
		{
			return;
		}
#endif // COMPLETION
		string quote_file_name = "Snapshot_lvt_SZ";
		if (!process<SDS20LEVEL2, Snapshot>(filename, quote_file_name, *Snapshot_lvt_sz, LvtToSnapshot, TdbToSnapshot, Snapshot2str))
		{
			return;
		}
		delete Snapshot_lvt_sz;
	};
	Trans_snapshot_lvt_sz();

	auto Trans_snapshot_lvt_sh = [&]() {
		cout << "开始转换Snapshot_lvt_sh数据..." << endl;
		Quote_Request<SDS20LEVEL2, Snapshot>*  Snapshot_lvt_sh = new Quote_Request<SDS20LEVEL2, Snapshot>;
		Snapshot_lvt_sh->set_date(date);

		string type = "tick";
		string market = "SH";
		string variety;
		string path;
		Snapshot_lvt_sh->set_market(market);
		variety = "stock";
		path = base_path + variety + "/" + type + "/" + market + "/" + date;
		Snapshot_lvt_sh->set_FileList(path, market, variety);
		variety = "bond";
		path = base_path + variety + "/" + type + "/" + market + "/" + date;
		Snapshot_lvt_sh->set_FileList(path, market, variety);
		variety = "fund";
		path = base_path + variety + "/" + type + "/" + market + "/" + date;
		Snapshot_lvt_sh->set_FileList(path, market, variety);
		string filename = "/UKData/TDF/sds/" + date;
		filename = FindFileName<Type::LVT>(filename);
#ifdef COMPLETION
		filename = FindFileName<Type::LVT>(filename);
		if (filename.empty())
		{
			return;
		}
#endif // COMPLETION
		string quote_file_name = "Snapshot_lvt_SH";
		if (!process<SDS20LEVEL2, Snapshot>(filename, quote_file_name, *Snapshot_lvt_sh, LvtToSnapshot, TdbToSnapshot, Snapshot2str))
		{
			return;
		}
		delete Snapshot_lvt_sh;
	};
	Trans_snapshot_lvt_sh();
}

void merge_order()
{
	set<string> markets;
#ifdef DEBUG
	Init_ukdb09(markets, true);
#else
	Init_ukdb09(markets);
#endif // DEBUG

	cout << "开始转换Order数据..." << endl;
	Quote_Request<SDS20ORDER, Order>*  ToOrder = new Quote_Request<SDS20ORDER, Order>;
	ToOrder->set_date(date);

	auto Trans_order = [&ToOrder]() {
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
	Trans_order();

	string filename = "/UKData/TDF/sds/" + date;
#ifdef COMPLETION
	filename = FindFileName<Type::ORD>(filename);
	if (filename.empty())
	{
		return;
	}
#endif // COMPLETION
	string quote_file_name = "Order";
	if (!process<SDS20ORDER, Order>(filename, quote_file_name, *ToOrder, OrdToOrder, TdbToOrder, Order2str))
	{
		return;
	}
	delete ToOrder;
}

void merge_orderqueue()
{
	set<string> markets;
#ifdef DEBUG
	Init_ukdb09(markets, true);
#else
	Init_ukdb09(markets);
#endif // DEBUG
	cout << "开始转换OrderQueue数据..." << endl;

	Quote_Request<SDS20ORDERQUEUE, OrderQueue>*  ToOrderQueue = new Quote_Request<SDS20ORDERQUEUE, OrderQueue>;
	ToOrderQueue->set_date(date);

	auto Trans_orderquque = [&ToOrderQueue]() {
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
	Trans_orderquque();

	string filename = "/UKData/TDF/sds/" + date;
#ifdef COMPLETION
	filename = FindFileName<Type::ORQ>(filename);
	if (filename.empty())
	{
		return;
	}
#endif // COMPLETION
	string quote_file_name = "OrderQueue";
	if (!process<SDS20ORDERQUEUE, OrderQueue>(filename, quote_file_name, *ToOrderQueue, OrqToOrderqueue, TdbToOrderQueue, Orderque2str))
	{
		return;
	}
	delete ToOrderQueue;
}

void merge_transaction()
{
	set<string> markets;
#ifdef DEBUG
	Init_ukdb09(markets, true);
#else
	Init_ukdb09(markets);
#endif // DEBUG
	cout << "开始转换Transaction数据..." << endl;

	Quote_Request<SDS20TRANSACTION, Transaction>*  ToTransaction = new Quote_Request<SDS20TRANSACTION, Transaction>;
	ToTransaction->set_date(date);

	auto Trans_Transaction = [&ToTransaction]() {
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
	Trans_Transaction();

	string filename = "/UKData/TDF/sds/" + date;
#ifdef COMPLETION
	filename = FindFileName<Type::TRD>(filename);
	if (filename.empty())
	{
		return;
	}
#endif // COMPLETION
	string quote_file_name = "Transaction";
	if (!process<SDS20TRANSACTION, Transaction>(filename, quote_file_name, *ToTransaction, TrdToTransaction, TdbToTransaction, Trans2str))
	{
		return;
	}
	delete ToTransaction;
}

void merge_snapshot_sort()
{
	string Path = PATH + date + "/";
	string Filesnap_0 = Path + date + "_Snapshot_lvt_SZ";
	string Filesnap_1 = Path + date + "_Snapshot_lvt_SH";
	string Filesnap_2 = Path + date + "_Snapshot_cfe";
	string Filesnap_3 = Path + date + "_Snapshot_idx";
	string Filesnap_4 = Path + date + "_Snapshot_spi";
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

	int64_t stepTime = 1 * 3600;//7200000000
	int64_t EndTime = MinTime + stepTime * 1000000;
	if (GetMsTime(atoi(date.c_str()) - 1, 80000000) > EndTime)
	{
		EndTime = GetMsTime(atoi(date.c_str()) - 1, 80000000);
	}
	merger_sort(FileList, EndTime, FileSouce);
	for (auto &it : FileList)
	{
		if (remove(it.first.c_str()))
		{
			cerr << "remove " << Filesnap_0 << " is error" << endl;
		}
	}
}

int main(int argc, char** argv)
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
		{ "TICK", merge_snapshot },
		{ "ORDER", merge_order },
		{ "ORDERQUEUE", merge_orderqueue },
		{ "TRANS", merge_transaction },
		{ "SORT", merge_snapshot_sort },
	};
	func[type]();
	delete content;
	return 0;
}