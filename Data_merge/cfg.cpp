#include "cfg.h"

map<int, string> MarketID{
	// 亚太地区
	//{ MARKET_ALL, "ALL" },
	{ MARKET_SZA, "SZA" },
	{ MARKET_SHA, "SHA" },
	{ MARKET_CFE, "CFX" },
	{ MARKET_SHF, "SHF" },
	{ MARKET_CZC, "CZC" },
	{ MARKET_DCE, "DCE" },
	{ MARKET_SGE, "SGE" },
	{ MARKET_SZB, "SZB" },
	{ MARKET_SHB, "SHB" },
	{ MARKET_HK, "HK" },
	{ MARKET_IBBM, "IBBM" },
	{ MARKET_OTC, "OTC" },
	{ MARKET_TAIFEX, "TAIFEX" },
	{ MARKET_SGX, "SGX" },
	{ MARKET_SICOM, "SICOM" },
	{ MARKET_JPX, "JPX" },
	{ MARKET_TOCOM, "TOCOM" },
	{ MARKET_BMD, "BMD" },
	{ MARKET_TFEX, "TFEX" },
	{ MARKET_AFET, "AFET" },
	{ MARKET_KRX, "KRX" },


	// 欧洲地区
	{ MARKET_LME, "LME" },
	{ MARKET_ICE, "ICE" },
	{ MARKET_LIFFE, "LIFFE" },
	//{ MARKET_XEurex, "XEurex" },

	// 美洲地区
	{ MARKET_CME, "CME" },
	{ MARKET_CBOT, "CBOT" },
	{ MARKET_NYBOT, "NYBOT" },
	{ MARKET_NYMEX_COMEX, "NYMEX_COMEX" },
	{ MARKET_ICE_CANOLA, "ICE_CANOLA" },
	{ MARKET_eCBOT, "eCBOT" },
	{ MARKET_CBOE, "CBOE" },

	// 其他地区
	{ MARKET_SFE, "SFE" },
	{ MARKET_DME, "DME" },
	{ MARKET_DGCX, "DGCX" },
};

map<int, string> VarID{
	//{ VARIETY_ALL, "all" },
	{ VARIETY_STOCK, "stock" },
	{ VARIETY_BOND, "bond" },
	{ VARIETY_FUND, "fund" },
	{ VARIETY_SPOT, "spot" },
	{ VARIETY_MONEY_MARKET, "money" },
	{ VARIETY_INDEX, "index" },
	{ VARIETY_FUTURE, "future" },
	{ VARIETY_OPTION, "option" },
	{ VARIETY_WARRANT, "warrant" },
	{ VARIETY_STOCK_OPTION, "stcopt" },
};

int64_t GetMsTime(int64_t ymd, int64_t hmsu)
{
	struct tm timeinfo = { 0 };
	time_t second;
	int64_t  usecond;
	timeinfo.tm_year = static_cast<int>(ymd / 10000 - 1900);
	timeinfo.tm_mon = static_cast<int>((ymd % 10000) / 100 - 1);
	timeinfo.tm_mday = static_cast<int>(ymd % 100);
	second = mktime(&timeinfo);
	//80000000

	int hou = static_cast<int>(hmsu / 10000000);
	int min = static_cast<int>((hmsu % 10000000) / 100000);
	int sed = static_cast<int>((hmsu % 100000) / 1000);
	int used = static_cast<int>(hmsu % 1000);

	usecond = second + hou * 3600 + min * 60 + sed;
	usecond *= 1000000;
	usecond += used * 1000;
	return usecond;
}

void Snapshot2str(Snapshot *ptr, string &str)
{
	stringstream ss;
	ss << ptr->ukey << ","
		<< ptr->trday << ","
		<< ptr->timeus << ","
		<< ptr->recvus << ","
		<< ptr->status << ","
		<< ptr->pre_close << ","
		<< ptr->high << ","
		<< ptr->low << ","
		<< ptr->open << ","
		<< ptr->last << ","
		<< ptr->match_num << ","
		<< ptr->volume << ","
		<< ptr->turnover << ",";

	for (int i = 0; i < 10; ++i)
		ss << ptr->info[i] << ",";
	for (int i = 0; i < 10; ++i)
		ss << ptr->ask_price[i] << ",";
	for (int i = 0; i < 10; ++i)
		ss << ptr->ask_volume[i] << ",";
	for (int i = 0; i < 10; ++i)
		ss << ptr->bid_price[i] << ",";
	for (int i = 0; i < 10; ++i)
		ss << ptr->bid_volume[i] << ",";
	ss << ptr->ask_orders_num << ","
		<< ptr->bid_orders_num << ",";
	for (int i = 0; i < 50; ++i)
		ss << ptr->ask_queue[i] << ",";
	for (int i = 0; i < 49; ++i)
		ss << ptr->bid_queue[i] << ",";
	ss << ptr->bid_queue[49] << endl;
	str = ss.str();
}

void Order2str(Order *ptr, string &str)
{
	stringstream ss;
	ss << ptr->ukey << ","
		<< ptr->trday << ","
		<< ptr->timeus << ","
		<< ptr->recvus << ","
		<< ptr->index << ","
		<< ptr->price << ","
		<< ptr->volume << ","
		<< ptr->order_type << endl;

	str = ss.str();
}

void Trans2str(Transaction* ptr, string &str)
{
	stringstream ss;
	ss << ptr->ukey << ","
		<< ptr->trday << ","
		<< ptr->timeus << ","
		<< ptr->recvus << ","
		<< ptr->index << ","
		<< ptr->price << ","
		<< ptr->volume << ","
		<< ptr->ask_order << ","
		<< ptr->bid_order << ","
		<< ptr->trade_type << endl;
	str = ss.str();
}

void Orderque2str(OrderQueue *ptr, string &str)
{
	stringstream ss;
	ss << ptr->ukey << ","
		<< ptr->trday << ","
		<< ptr->timeus << ","
		<< ptr->recvus << ","
		<< ptr->side << ","
		<< ptr->price << ","
		<< ptr->orders_num << ",";
	for (int i = 0; i < 49; ++i)
		ss << ptr->queue[i] << ",";
	ss << ptr->queue[49] << endl;
	str = ss.str();
}

void SH_IndexCodeTrans(char *ChCode)
{
	switch (atoi(ChCode))
	{
	case 999999:
		memcpy(ChCode, "000001", 6);
		break;
	case 999998:
		memcpy(ChCode, "000002", 6);
		break;
	case 999997:
		memcpy(ChCode, "000003", 6);
		break;
	case 999996:
		memcpy(ChCode, "000004", 6);
		break;
	case 999995:
		memcpy(ChCode, "000005", 6);
		break;
	case 999994:
		memcpy(ChCode, "000006", 6);
		break;
	case 999993:
		memcpy(ChCode, "000007", 6);
		break;
	case 999992:
		memcpy(ChCode, "000008", 6);
		break;
	case 999991:
		memcpy(ChCode, "000010", 6);
		break;
	case 999990:
		memcpy(ChCode, "000011", 6);
		break;
	case 999989:
		memcpy(ChCode, "000012", 6);
		break;
	case 999988:
		memcpy(ChCode, "000013", 6);
		break;
	case 999987:
		memcpy(ChCode, "000016", 6);
		break;
	case 999986:
		memcpy(ChCode, "000015", 6);
		break;
	default:
		if (strlen(ChCode) > 2)
			memcpy(ChCode, "00", 2);
		break;
	}
}

bool LvtToSnapshot(Snapshot& OutPut, SDS20LEVEL2& Input)
{
	string windcode = string(Input.szWindCode);
	int64_t ukey = GetUkey(windcode.substr(windcode.rfind('.') + 1), windcode.substr(0, windcode.rfind('.')));
	if (ukey == 0)
	{
		return false;
	}
	else
	{
		OutPut.ukey = ukey;
		OutPut.trday = Input.nActionDay;
		OutPut.timeus = GetMsTime(OutPut.trday, Input.nTime);
		OutPut.recvus = (Input.nRecvTime < Input.nTime ? OutPut.timeus : GetMsTime(OutPut.trday, Input.nRecvTime));
		OutPut.status = 0;
		OutPut.pre_close = Input.nPreClose;
		OutPut.high = Input.nHigh;
		OutPut.low = Input.nLow;
		OutPut.open = Input.nOpen;
		OutPut.last = Input.nMatch;
		OutPut.match_num = Input.nNumTrades;
		OutPut.volume = Input.iVolume;
		OutPut.turnover = Input.iTurnover;
		OutPut.info[0] = Input.nHighLimited;
		OutPut.info[1] = Input.nLowLimited;
		OutPut.info[2] = Input.nTotalBidVol;
		OutPut.info[3] = Input.nTotalAskVol;
		OutPut.info[4] = Input.nWeightedAvgBidPrice;
		OutPut.info[5] = Input.nWeightedAvgAskPrice;
		OutPut.info[6] = Input.nIOPV;
		OutPut.info[7] = Input.nYieldToMaturity;
		OutPut.info[8] = Input.nSyl1;
		OutPut.info[9] = Input.nSyl2;
		for (int i = 0; i < 10; ++i)
		{
			OutPut.ask_price[i] = Input.nAskPrice[i];
		}
		for (int i = 0; i < 10; ++i)
		{
			OutPut.ask_volume[i] = Input.nAskVol[i];
		}
		for (int i = 0; i < 10; ++i)
		{
			OutPut.bid_price[i] = Input.nBidPrice[i];
		}
		for (int i = 0; i < 10; ++i)
		{
			OutPut.bid_volume[i] = Input.nBidVol[i];
		}
		return true;
	}
}

bool IdxToSnapshot(Snapshot& OutPut, SDS20INDEX& InPut)
{
	char   code[32]{ 0 };
	strncpy(code, InPut.szWindCode, 32);
	if (0 == strcasecmp(code + 7, "SH"))
	{
		SH_IndexCodeTrans(code);
	}
	string windcode = code;
	int64_t ukey = GetUkey(windcode.substr(windcode.rfind('.') + 1), windcode.substr(0, windcode.rfind('.')));
	if (0 == ukey)
	{
		return false;
	}
	else
	{
		OutPut.ukey = ukey;
		OutPut.trday = InPut.nActionDay;
		OutPut.timeus = GetMsTime(OutPut.trday, InPut.nTime);
		OutPut.recvus = (InPut.nRecvTime < InPut.nTime ? OutPut.timeus : GetMsTime(OutPut.trday, InPut.nRecvTime));
		OutPut.status = 0;
		OutPut.pre_close = InPut.nPreCloseIndex;
		OutPut.high = InPut.nHighIndex;
		OutPut.low = InPut.nLowIndex;
		OutPut.open = InPut.nOpenIndex;
		OutPut.last = InPut.nLastIndex;
		OutPut.match_num = 0;
		OutPut.volume = InPut.iTotalVolume;
		OutPut.turnover = InPut.iTurnover;
		return true;
	}
}

bool Cfe_SpiToSnapshot(Snapshot& OutPut, SDS20FUTURE& InPut)
{
	string windcode = string(InPut.szWindCode);
	int64_t ukey = GetUkey(windcode.substr(windcode.rfind('.') + 1), windcode.substr(0, windcode.rfind('.')));
	if (0 == ukey)
	{
		return false;
	}
	else
	{
		OutPut.ukey = ukey;
		OutPut.trday = InPut.nActionDay;
		OutPut.timeus = GetMsTime(OutPut.trday, InPut.nTime);
		OutPut.recvus = (InPut.nRecvTime < InPut.nTime ? OutPut.timeus : GetMsTime(OutPut.trday, InPut.nRecvTime));
		OutPut.status = 0;
		OutPut.pre_close = InPut.nPreClose;
		OutPut.high = InPut.nHigh;
		OutPut.low = InPut.nLow;
		OutPut.open = InPut.nOpen;
		OutPut.last = InPut.nMatch;
		OutPut.match_num = 0;
		OutPut.volume = InPut.iVolume;
		OutPut.turnover = InPut.iTurnover;
		OutPut.info[0] = InPut.nHighLimited;
		OutPut.info[1] = InPut.nLowLimited;
		OutPut.info[2] = InPut.nSettlePrice;
		OutPut.info[3] = InPut.nPreSettlePrice;
		OutPut.info[4] = InPut.iOpenInterest;
		OutPut.info[5] = InPut.iPreOpenInterest;
		OutPut.info[6] = InPut.nCurrDelta;
		OutPut.info[7] = InPut.nPreDelta;
		OutPut.info[8] = 0;
		OutPut.info[9] = 0;
		for (int i = 0; i < 10; ++i)
		{
			if (i < 5)
			{
				OutPut.ask_price[i] = InPut.nAskPrice[i];
			}
			else
			{
				OutPut.ask_price[i] = 0;
			}
		}
		for (int i = 0; i < 10; ++i)
		{
			if (i < 5)
			{
				OutPut.ask_volume[i] = InPut.nAskVol[i];
			}
			else
			{
				OutPut.ask_volume[i] = 0;
			}
		}
		for (int i = 0; i < 10; ++i)
		{
			if (i < 5)
			{
				OutPut.bid_price[i] = InPut.nBidPrice[i];
			}
			else
			{
				OutPut.bid_price[i] = 0;
			}
		}
		for (int i = 0; i < 10; ++i)
		{
			if (i < 5)
			{
				OutPut.bid_volume[i] = InPut.nBidVol[i];
			}
			else
			{
				OutPut.bid_volume[i] = 0;
			}
		}
		return true;
	}
}

bool TrdToTransaction(Transaction& OutPut, SDS20TRANSACTION& InPut)
{
	string windcode = string(InPut.szWindCode);
	int64_t ukey = GetUkey(windcode.substr(windcode.rfind('.') + 1), windcode.substr(0, windcode.rfind('.')));
	if (0 == ukey)
	{
		return false;
	}
	else
	{
		OutPut.ukey = ukey;
		OutPut.trday = InPut.nActionDay;
		OutPut.timeus = GetMsTime(OutPut.trday, InPut.nTime);
		OutPut.recvus = (InPut.nRecvTime < InPut.nTime ? OutPut.timeus : GetMsTime(OutPut.trday, InPut.nRecvTime));
		OutPut.index = InPut.nIndex;
		OutPut.price = InPut.nPrice;
		OutPut.volume = InPut.nVolume;
		OutPut.ask_order = InPut.nAskOrder;
		OutPut.bid_order = InPut.nBidOrder;
		OutPut.trade_type = make_trade_type((char)InPut.nBSFlag, InPut.chOrderKind, InPut.chFunctionCode);
		return true;
	}
}

bool OrdToOrder(Order& OutPut, SDS20ORDER& InPut)
{
	string windcode = string(InPut.szWindCode);
	int64_t ukey = GetUkey(windcode.substr(windcode.rfind('.') + 1), windcode.substr(0, windcode.rfind('.')));
	if (0 == ukey)
	{
		return false;
	}
	else
	{
		OutPut.ukey = ukey;
		OutPut.trday = InPut.nActionDay;
		OutPut.timeus = GetMsTime(OutPut.trday, InPut.nTime);
		OutPut.recvus = (InPut.nRecvTime < InPut.nTime ? OutPut.timeus : GetMsTime(OutPut.trday, InPut.nRecvTime));
		OutPut.index = InPut.nOrder;
		OutPut.price = InPut.nPrice;
		OutPut.volume = InPut.nVolume;
		OutPut.order_type = make_order_type(InPut.chOrderKind, InPut.chFunctionCode);
		return true;
	}
}

bool OrqToOrderqueue(OrderQueue& OutPut, SDS20ORDERQUEUE& InPut)
{
	string windcode = string(InPut.szWindCode);
	int64_t ukey = GetUkey(windcode.substr(windcode.rfind('.') + 1), windcode.substr(0, windcode.rfind('.')));
	if (0 == ukey)
	{
		return false;
	}
	else
	{
		OutPut.ukey = ukey;
		OutPut.trday = InPut.nActionDay;
		OutPut.timeus = GetMsTime(OutPut.trday, InPut.nTime);
		OutPut.recvus = (InPut.nRecvTime < InPut.nTime ? OutPut.timeus : GetMsTime(OutPut.trday, InPut.nRecvTime));
		OutPut.side = InPut.nSide;
		OutPut.price = InPut.nPrice;
		OutPut.orders_num = InPut.nOrders;
		for (int i = 0; i < 50; ++i)
		{
			if (i < InPut.nABItems)
			{
				OutPut.queue[i] = InPut.nABVolume[i];
			}
			else
			{
				OutPut.queue[i] = 0;
			}
		}
		return true;
	}
}

extern char  *content;
bool DeCompress(const std::string& filename, vector<string>& Vec)
{
	BZFILE *bz = nullptr;
	int bzerr;
	int ret = 0;
	int max_len = 1024 * 1024 * 10;
	if (filename.empty())
	{
		cerr << filename << " not exist " << endl;
		return false;
	}


	stringstream ss;
	bz = BZ2_bzopen(filename.c_str(), "r");
	if (bz == nullptr)
	{
		cerr << "bz2_open error" << endl;
		return false;
	}
	while (true)
	{
		ret = BZ2_bzRead(&bzerr, bz, (void*)content, max_len);
		ss.write((char*)content, ret);
		if (bzerr != BZ_OK || bzerr == BZ_STREAM_END)
		{
			break;
		}
	}
	BZ2_bzclose(bz);
	string line;
	if (bzerr == BZ_STREAM_END)
	{
		while (getline(ss, line))
		{
			if (line.empty())
			{
				break;
			}
			Vec.emplace_back(line);
			line.clear();
		}
	}
	else
	{
		cerr << filename << "  decompress bad!" << endl;
		return false;
	}
	return true;
}

void TdbToSnapshot(int64_t ukey, vector<string>& Vec_Str, list<shared_ptr<Snapshot>>& L_Snap)
{
	if (Vec_Str.empty())
	{
		return;
	}
	string MyNULL;
	stringstream ss;
	int type = 0, market = 0;
	get_variety_market_by_ukey(ukey, type, market);
	for (auto &it : Vec_Str)
	{
		shared_ptr<Snapshot> temp_shared_ptr(new Snapshot);
		Snapshot *temp = temp_shared_ptr.get();
		replace(it.begin(), it.end(), ' ', '%');
		replace(it.begin(), it.end(), ',', ' ');
		memset(temp, 0, sizeof(Snapshot));
		ss.clear();
		ss.str(it);
		temp->ukey = ukey;
		ss >> MyNULL;
		ss >> MyNULL;
		ss >> temp->trday;
		ss >> temp->timeus;
		temp->timeus = GetMsTime(temp->trday, temp->timeus);
		temp->recvus = temp->timeus;
		if (type == VARIETY_FUTURE || type == VARIETY_INDEX)
		{
			ss >> temp->last;
		}
		ss >> MyNULL;
		ss >> MyNULL;
		ss >> temp->match_num;
		if (type == VARIETY_FUND || type == VARIETY_BOND || type == VARIETY_STOCK)
		{
			ss >> temp->info[6];
		}
		else
		{
			ss >> MyNULL;
		}
		ss >> MyNULL;
		ss >> MyNULL;
		ss >> temp->volume;
		ss >> temp->turnover;
		ss >> temp->high;
		ss >> temp->low;
		if (type == VARIETY_STOCK || type == VARIETY_BOND || type == VARIETY_FUND)
		{
			ss >> temp->last;
		}
		ss >> temp->open;
		ss >> temp->pre_close;
		if (type == VARIETY_FUTURE)
		{
			ss >> temp->info[2];
			ss >> temp->info[4];
			ss >> temp->info[6];
			ss >> temp->info[3];
			ss >> temp->info[5];
		}
		for (int i = 0; i < 10; ++i)
		{
			ss >> temp->ask_price[i];
		}
		for (int i = 0; i < 10; ++i)
		{
			ss >> temp->ask_volume[i];
		}
		for (int i = 0; i < 10; ++i)
		{
			ss >> temp->bid_price[i];
		}
		for (int i = 0; i < 10; ++i)
		{
			ss >> temp->bid_volume[i];
		}
		if (type == VARIETY_STOCK || type == VARIETY_BOND || type == VARIETY_FUND)
		{
			ss >> temp->info[5];
			ss >> temp->info[4];
			ss >> temp->info[3];
			ss >> temp->info[2];
		}
		else
		{
			ss >> MyNULL;
			ss >> MyNULL;
			ss >> MyNULL;
			ss >> MyNULL;
		}

		if (type == VARIETY_INDEX)
		{
			ss >> temp->info[0];
			ss >> temp->info[1];
			ss >> temp->info[2];
			ss >> temp->info[3];
			ss >> temp->info[4];
		}
		L_Snap.emplace_back(temp_shared_ptr);
	}
}

void TdbToOrder(int64_t ukey, vector<string>& Vec_Str, list<shared_ptr<Order>>& L_Snap)
{
	if (Vec_Str.empty())
	{
		return;
	}
	string MyNULL;
	stringstream ss;
	char  order_kind, function_code;
	for (auto &it : Vec_Str)
	{
		shared_ptr<Order> temp_shared_ptr(new Order);
		Order *temp = temp_shared_ptr.get();
		replace(it.begin(), it.end(), ' ', '%');
		replace(it.begin(), it.end(), ',', ' ');
		memset(temp, 0, sizeof(Order));
		ss.clear();
		ss.str(it);

		temp->ukey = ukey;
		ss >> MyNULL;
		ss >> MyNULL;
		ss >> temp->trday;
		ss >> temp->timeus;
		temp->timeus = GetMsTime(temp->trday, temp->timeus);
		temp->recvus = temp->timeus;
#ifndef COMPLETION
		ss >> temp->index;
#else
		ss >> MyNULL;
#endif // !COMPLETION
		ss >> MyNULL;
		ss >> order_kind;
		if (order_kind == '%')
		{
			order_kind = '\0';
		}
		ss >> function_code;
		if (function_code == '%')
		{
			function_code = '\0';
		}
		ss >> temp->price;
		ss >> temp->volume;
		temp->order_type = make_order_type(order_kind, function_code);
		L_Snap.emplace_back(temp_shared_ptr);
	}
}

void TdbToOrderQueue(int64_t ukey, vector<string>& Vec_Str, list<shared_ptr<OrderQueue>>& L_Snap)
{
	if (Vec_Str.empty())
	{
		return;
	}
	string MyNULL;
	stringstream ss;
	int count = 0;
	for (auto &it : Vec_Str)
	{
		shared_ptr<OrderQueue> temp_shared_ptr(new OrderQueue);
		OrderQueue *temp = temp_shared_ptr.get();
		replace(it.begin(), it.end(), ' ', '%');
		replace(it.begin(), it.end(), ',', ' ');
		memset(temp, 0, sizeof(OrderQueue));
		ss.clear();
		ss.str(it);
		count = 0;
		temp->ukey = ukey;
		ss >> MyNULL;
		ss >> MyNULL;
		ss >> temp->trday;
		ss >> temp->timeus;
		temp->timeus = GetMsTime(temp->trday, temp->timeus);
		temp->recvus = temp->timeus;
		ss >> temp->side;
		ss >> temp->price;
		ss >> temp->orders_num;
		ss >> count;
		for (auto i = 0; i < 50; ++i)
		{
			if (i < count)
			{
				ss >> temp->queue[i];
			}
			else
			{
				ss >> MyNULL;
			}
		}
		L_Snap.emplace_back(temp_shared_ptr);
	}
}

void TdbToTransaction(int64_t ukey, vector<string>& Vec_Str, list<shared_ptr<Transaction>>& L_Snap)
{
	if (Vec_Str.empty())
	{
		return;
	}
	string MyNULL;
	stringstream ss;
	char bs_flag, order_kind, function_code;
	for (auto &it : Vec_Str)
	{
		shared_ptr<Transaction> temp_shared_ptr(new Transaction);
		Transaction *temp = temp_shared_ptr.get();
		replace(it.begin(), it.end(), ' ', '%');
		replace(it.begin(), it.end(), ',', ' ');
		memset(temp, 0, sizeof(Transaction));
		ss.clear();
		ss.str(it);
		temp->ukey = ukey;
		ss >> MyNULL;
		ss >> MyNULL;
		ss >> temp->trday;
		ss >> temp->timeus;
		temp->timeus = GetMsTime(temp->trday, temp->timeus);
		temp->recvus = temp->timeus;
#ifndef COMPLETION
		ss >> temp->index;
#else
		ss >> MyNULL;
#endif // !COMPLETION
		ss >> function_code;
		if ('%' == function_code)
		{
			function_code = '\0';
		}
		ss >> order_kind;
		if ('%' == order_kind)
		{
			order_kind = '\0';
		}
		ss >> bs_flag;
		if ('%' == bs_flag)
		{
			bs_flag = '\0';
		}
		ss >> temp->price;
		ss >> temp->volume;
		ss >> temp->ask_order;
		ss >> temp->bid_order;
		temp->trade_type = make_trade_type(bs_flag, order_kind, function_code);

		L_Snap.emplace_back(temp_shared_ptr);
	}
}

bool merger_sort(map<string, int64_t> FileList, int64_t EndTime, string FileName)
{
	cout << EndTime << endl;
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
	merger_sort(FileList, EndTime + 3600000000, FileName);
	return true;
}

void MySort(list<Snapshot> &v_data, string FileName)
{
	list<Snapshot*>v_ptr;
	cout << v_data.size() << endl;
	//sleep(1);
	for (auto &it : v_data)
	{
		v_ptr.emplace_back(&it);
	}
	v_ptr.sort(compare_quote<Snapshot>());
	ofstream fout(FileName, ios_base::app);
	if (!fout.is_open())
		return;
	for (auto &it : v_ptr)
	{
		fout.write((const char *)it, sizeof(Snapshot));
	}
	fout.close();
}
