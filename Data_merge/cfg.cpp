#include "cfg.h"

const string base_path = "/data/UKData/Request.csv/";

map<int, string> MarketID{
	// 亚太地区
	//{ MARKET_ALL, "ALL" },
	{ MARKET_SZA, "SZA" },
	{ MARKET_SHA, "SHA" },
	{ MARKET_CFX, "CFX" },
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
	timeinfo.tm_year = ymd / 10000 - 1900;
	timeinfo.tm_mon = (ymd % 10000) / 100 - 1;
	timeinfo.tm_mday = ymd % 100;
	second = mktime(&timeinfo);
	//80000000
	int hou = hmsu / 10000000;
	int min = (hmsu % 10000000) / 100000;
	int sed = (hmsu % 100000) / 1000;
	int used = hmsu % 1000;

	usecond = second + hou * 3600 + min * 60 + sed;
	usecond *= 1000000;
	usecond += used * 1000;
	return usecond;
}

time_t GetTr_time()
{
	time_t tt;
	struct tm* pstm = nullptr;
	tt = time(nullptr);
	pstm = localtime(&tt);
	if ((pstm->tm_hour == SPLIT_HROUS && pstm->tm_min > SPLIT_MINN) || (pstm->tm_hour > SPLIT_HROUS))
	{
		tt += 24 * 60 * 60;
		pstm = localtime(&tt);
	}
	return tt;
}

int GetTrday()
{
	time_t tt;
	struct tm*	pstm = nullptr;
	tt = time(nullptr);
	pstm = localtime(&tt);
	if ((pstm->tm_hour == SPLIT_HROUS && pstm->tm_min > SPLIT_MINN) || (pstm->tm_hour > SPLIT_HROUS))
	{
		tt += 24 * 60 * 60;
		pstm = localtime(&tt);
	}
	int year = pstm->tm_year + 1900;
	int mon = pstm->tm_mon + 1;
	int day = pstm->tm_mday;
	return year * 10000 + mon * 100 + day;
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

bool DeCompress(const std::string& filename, vector<string>& Vec)
{
	BZFILE *bz = nullptr;
	int bzerr;
	int ret = 0;
	const int max_len = 1024 * 1024 * 10;
	char content[max_len];
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

void TdbToSnapshot(int64_t ukey, vector<string>& Vec_Str, list<Snapshot>& L_Snap)
{
	if (Vec_Str.empty())
	{
		return;
	}
	Snapshot temp = { 0 };
	string MyNULL;
	stringstream ss;
	int type = 0, market = 0;
	get_variety_market_by_ukey(ukey, type, market);
	for (auto &it : Vec_Str)
	{
		replace(it.begin(), it.end(), ' ', '%');
		replace(it.begin(), it.end(), ',', ' ');
		memset((void*)&temp, 0, sizeof(temp));
		ss.clear();
		ss.str(it);
		temp.ukey = ukey;
		ss >> MyNULL;
		ss >> MyNULL;
		ss >> temp.trday;
		ss >> temp.timeus;
		temp.timeus = GetMsTime(temp.trday, temp.timeus);
		temp.recvus = temp.timeus;
		if (type == VARIETY_FUTURE || type == VARIETY_INDEX)
		{
			ss >> temp.last;
		}
		ss >> MyNULL;
		ss >> MyNULL;
		ss >> temp.match_num;
		if (type == VARIETY_FUND || type == VARIETY_BOND || type == VARIETY_STOCK)
		{
			ss >> temp.info[6];
		}
		else
		{
			ss >> MyNULL;
		}
		ss >> MyNULL;
		ss >> MyNULL;
		ss >> temp.volume;
		ss >> temp.turnover;
		ss >> temp.high;
		ss >> temp.low;
		if (type == VARIETY_STOCK || type == VARIETY_BOND || type == VARIETY_FUND)
		{
			ss >> temp.last;
		}
		ss >> temp.open;
		ss >> temp.pre_close;
		if (type == VARIETY_FUTURE)
		{
			ss >> temp.info[2];
			ss >> temp.info[4];
			ss >> temp.info[6];
			ss >> temp.info[3];
			ss >> temp.info[5];
		}
		for (int i = 0; i < 10; ++i)
		{
			ss >> temp.ask_price[i];
		}
		for (int i = 0; i < 10; ++i)
		{
			ss >> temp.ask_volume[i];
		}
		for (int i = 0; i < 10; ++i)
		{
			ss >> temp.bid_price[i];
		}
		for (int i = 0; i < 10; ++i)
		{
			ss >> temp.bid_volume[i];
		}
		if (type == VARIETY_STOCK || type == VARIETY_BOND || type == VARIETY_FUND)
		{
			ss >> temp.info[5];
			ss >> temp.info[4];
			ss >> temp.info[3];
			ss >> temp.info[2];
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
			ss >> temp.info[0];
			ss >> temp.info[1];
			ss >> temp.info[2];
			ss >> temp.info[3];
			ss >> temp.info[4];
		}
		L_Snap.emplace_back(temp);
	}
}

void TdbToSnapshot_include_orderque(int64_t ukey, vector<string>& Vec_Str, list<Snapshot>& L_Snap, map<int64_t, OrderQueue>& AskQue, map<int64_t, OrderQueue>& BidQue)
{
	if (Vec_Str.empty())
	{
		return;
	}
	Snapshot		temp = { 0 };
	string			MyNULL;
	stringstream	ss;
	int type = 0, market = 0;
	get_variety_market_by_ukey(ukey, type, market);
	for (auto &it : Vec_Str)
	{
		replace(it.begin(), it.end(), ' ', '%');
		replace(it.begin(), it.end(), ',', ' ');
		memset((void*)&temp, 0, sizeof(temp));
		ss.clear();
		ss.str(it);
		temp.ukey = ukey;
		ss >> MyNULL;
		ss >> MyNULL;
		ss >> temp.trday;
		ss >> temp.timeus;
		temp.timeus = GetMsTime(temp.trday, temp.timeus);
		temp.recvus = temp.timeus;
		if (type == VARIETY_FUTURE || type == VARIETY_INDEX)
		{
			ss >> temp.last;
		}
		ss >> MyNULL;
		ss >> MyNULL;
		ss >> temp.match_num;
		if (type == VARIETY_FUND || type == VARIETY_BOND || type == VARIETY_STOCK)
		{
			ss >> temp.info[6];
		}
		else
		{
			ss >> MyNULL;
		}
		ss >> MyNULL;
		ss >> MyNULL;
		ss >> temp.volume;
		ss >> temp.turnover;
		ss >> temp.high;
		ss >> temp.low;
		if (type == VARIETY_STOCK || type == VARIETY_BOND || type == VARIETY_FUND)
		{
			ss >> temp.last;
		}
		ss >> temp.open;
		ss >> temp.pre_close;
		if (type == VARIETY_FUTURE)
		{
			ss >> temp.info[2];
			ss >> temp.info[4];
			ss >> temp.info[6];
			ss >> temp.info[3];
			ss >> temp.info[5];
		}
		for (int i = 0; i < 10; ++i)
		{
			ss >> temp.ask_price[i];
		}
		for (int i = 0; i < 10; ++i)
		{
			ss >> temp.ask_volume[i];
		}
		for (int i = 0; i < 10; ++i)
		{
			ss >> temp.bid_price[i];
		}
		for (int i = 0; i < 10; ++i)
		{
			ss >> temp.bid_volume[i];
		}
		if (type == VARIETY_STOCK || type == VARIETY_BOND || type == VARIETY_FUND)
		{
			ss >> temp.info[5];
			ss >> temp.info[4];
			ss >> temp.info[3];
			ss >> temp.info[2];
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
			ss >> temp.info[0];
			ss >> temp.info[1];
			ss >> temp.info[2];
			ss >> temp.info[3];
			ss >> temp.info[4];
		}

		if (AskQue.empty() && BidQue.empty())
		{
			L_Snap.emplace_back(temp);
		}
		else
		{
			int bn = 0, an = 0;	// 订单数量
			int64_t *ba = NULL, *aa = NULL;
			if (BidQue.find(temp.timeus) != BidQue.end())
			{
				auto poq = BidQue[temp.timeus];
				bn = poq.orders_num;
				ba = poq.queue;
			}
			if (AskQue.find(temp.timeus) != AskQue.end())
			{
				auto poq = AskQue[temp.timeus];
				an = poq.orders_num;
				aa = poq.queue;
			}
			temp.ask_orders_num = an;
			temp.bid_orders_num = bn;
			if (aa != NULL)
			{
				for (int i = 0; i < 50; ++i)
				{
					temp.ask_queue[i] = aa[i];
				}
			}
			if (ba != NULL)
			{
				for (int i = 0; i < 50; ++i)
				{
					temp.bid_queue[i] = ba[i];
				}
			}
			L_Snap.emplace_back(temp);
		}
	}
}

void TdbToOrder(int64_t ukey, vector<string>& Vec_Str, list<Order>& L_Snap)
{
	if (Vec_Str.empty())
	{
		return;
	}
	Order temp = { 0 };
	string MyNULL;
	stringstream ss;
	char  order_kind, function_code;
	for (auto &it : Vec_Str)
	{
		replace(it.begin(), it.end(), ' ', '%');
		replace(it.begin(), it.end(), ',', ' ');
		memset((void*)&temp, 0, sizeof(temp));
		ss.clear();
		ss.str(it);

		temp.ukey = ukey;
		ss >> MyNULL;
		ss >> MyNULL;
		ss >> temp.trday;
		ss >> temp.timeus;
		temp.timeus = GetMsTime(temp.trday, temp.timeus);
		temp.recvus = temp.timeus;
		ss >> temp.index;
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
		ss >> temp.price;
		ss >> temp.volume;
		temp.order_type = make_order_type(order_kind, function_code);
		L_Snap.emplace_back(temp);
	}
}

void TdbToOrderQueue(int64_t ukey, vector<string>& Vec_Str, list<OrderQueue>& L_Snap)
{
	if (Vec_Str.empty())
	{
		return;
	}
	OrderQueue temp = { 0 };
	string MyNULL;
	stringstream ss;
	int count = 0;
	for (auto &it : Vec_Str)
	{
		replace(it.begin(), it.end(), ' ', '%');
		replace(it.begin(), it.end(), ',', ' ');
		memset((void*)&temp, 0, sizeof(temp));
		ss.clear();
		ss.str(it);
		count = 0;
		temp.ukey = ukey;
		ss >> MyNULL;
		ss >> MyNULL;
		ss >> temp.trday;
		ss >> temp.timeus;
		temp.timeus = GetMsTime(temp.trday, temp.timeus);
		temp.recvus = temp.timeus;
		ss >> temp.side;
		ss >> temp.price;
		ss >> temp.orders_num;
		ss >> count;
		for (auto i = 0; i < 50; ++i)
		{
			if (i < count)
			{
				ss >> temp.queue[i];
			}
			else
			{
				ss >> MyNULL;
			}
		}
		L_Snap.emplace_back(temp);
	}
}

void TdbToTransaction(int64_t ukey, vector<string>& Vec_Str, list<Transaction>& L_Snap)
{
	if (Vec_Str.empty())
	{
		return;
	}
	Transaction temp = { 0 };
	string MyNULL;
	stringstream ss;
	char bs_flag, order_kind, function_code;
	for (auto &it : Vec_Str)
	{
		replace(it.begin(), it.end(), ' ', '%');
		replace(it.begin(), it.end(), ',', ' ');
		memset((void*)&temp, 0, sizeof(temp));
		ss.clear();
		ss.str(it);
		temp.ukey = ukey;
		ss >> MyNULL;
		ss >> MyNULL;
		ss >> temp.trday;
		ss >> temp.timeus;
		temp.timeus = GetMsTime(temp.trday, temp.timeus);
		temp.recvus = temp.timeus;
		ss >> temp.index;
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
		ss >> temp.price;
		ss >> temp.volume;
		ss >> temp.ask_order;
		ss >> temp.bid_order;
		temp.trade_type = make_trade_type(bs_flag, order_kind, function_code);

		L_Snap.emplace_back(temp);
	}
}

void MakeDictionary(vector<string>& Str_orderque, int64_t ukey, map<int64_t, OrderQueue>& AskQue, map<int64_t, OrderQueue>& BidQue)
{
	OrderQueue		temp = { 0 };
	string			MyNULL;
	stringstream	ss;
	AskQue.clear();
	BidQue.clear();
	for (auto &it : Str_orderque)
	{
		short int	ABtem = 0;
		replace(it.begin(), it.end(), ' ', '%');
		replace(it.begin(), it.end(), ',', ' ');
		ss.clear();
		temp.ukey = ukey;
		ss.str(it);
		ss >> MyNULL;				//万得代码(AG1312.SHF)
		ss >> MyNULL;				//交易所代码(ag1312)
		ss >> temp.trday;			//日期（自然日）格式YYMMDD
		ss >> temp.timeus;			//订单时间(精确到毫秒HHMMSSmmm)
		temp.timeus = GetMsTime(temp.trday, temp.timeus);
		ss >> temp.side;			 //买卖方向('B':Bid 'A':Ask)
		ss >> temp.price;			 //成交价格((a double number + 0.00005) *10000)
		ss >> temp.orders_num;		 //订单数量
		ss >> ABtem;				 //明细个数
		for (int i = 0; i < 50; ++i)
		{
			if (i < ABtem)
			{
				ss >> temp.queue[i];	//订单明细
			}
			else
			{
				ss >> MyNULL;
			}
		}

		if (temp.side == 'B')
		{
			BidQue.emplace(temp.timeus, temp);
		}
		else
		{
			AskQue.emplace(temp.timeus, temp);
		}
	}
}

template<typename T>
bool WriteToCvs(map<int64_t, list<T>>& ukey_list_data, string& date, function<void(T*, string&)> Trans2str)
{
	if (ukey_list_data.empty())
	{
		return false;
	}
	for (auto &data : ukey_list_data)
	{
		string request_name = base_path;
		int64_t ukey = data.begin().ukey;
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
			request_name += MarketID[mk];
			request_name += "/";
			request_name += VarID[ty];
			request_name += "/";
			request_name += itostr(ukey);
			request_name += "/";
			request_name += date.substr(0, 4);
			request_name += "/";
			request_name += date;
			request_name += "/";
			while (!mkdir_path(request_name))
			{
				cout << "waitting mkdir " << request_name << endl;
			}
			request_name += Request_FileName[typeid(T).name()];
			ofstream fout_csv(request_name, ios::app);
			if (!fout_csv.is_open())
			{
				cerr << "create " << request_name << "  error" << endl;
				return false;
			}
			string str;
			for (auto &it : data)
			{
				str.clear();
				ToStr(&it, str);
				fout_csv << str;
			}
			fout_csv.close();
			return true;
		}
		else
		{
			return false;
		}
	}
}

template<typename T>
bool SortAndWrite(map<int64_t, list<T>>& ukey_list_data, string& FileName,string& date)
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
	for (auto &it: ukey_list_data)
	{
		for (auto &te:it.second)
		{
			v_ptr.emplace_back(&it);
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
	FileHead.recnum = v_ptr.size();
	FileHead.updatetm = time(NULL);
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