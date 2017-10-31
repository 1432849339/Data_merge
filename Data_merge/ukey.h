#ifndef __HONGKING_UK_API_
#define __HONGKING_UK_API_
#include <stdint.h>
#include <vector>
#include <map>
#include <set>
#pragma  pack(push)
#pragma pack(1)
namespace chronos
{
	//返回消息定义------------------------------------------------
	enum MsgType
	{
		ID_INTERNET_ERR							//网络错误
		//....
	};

	//交易所ID定义------------------------------------------------
	enum MarketType
	{
		//亚太地区
		MARKET_ALL = 0,
		MARKET_SZA,								//深圳交易所A股
		MARKET_SHA,								//上海交易所A股
		MARKET_CFX,								//中国金融期货交易所
		MARKET_SHF,								//上海金属期货交易所
		MARKET_CZC,								//郑州商品交易所
		MARKET_DCE,								//大连商品交易所
		MARKET_SGE,								//上海黄金交易所
		MARKET_SZB,								//深圳交易所B股
		MARKET_SHB,								//上海交易所B股
		MARKET_HK,								//香港联合交易所
		MARKET_IBBM,							//银行间债券市场
		MARKET_OTC = 15,						//中国股转系统(三版交易)
		MARKET_TAIFEX = 16,                     //台湾期货交易所
		MARKET_SGX = 20,						//新加坡交易所
		MARKET_SICOM = 21,						//新加坡商品交易所
		MARKET_JPX = 30,                        //日本交易所集团
		MARKET_TOCOM = 31,						//东京商品交易所
		MARKET_BMD = 40,						//马来西亚衍生品交易所
		MARKET_TFEX = 50,                       //泰国期货交易所
		MARKET_AFET = 51,						//泰国农业期货交易所
		MARKET_KRX = 60,                        //韩国交易所


		//欧洲地区
		MARKET_LME = 257,						//伦敦金属交易所
		MARKET_ICE = 258,						//伦敦洲际交易所
		MARKET_LIFFE = 259,						//伦敦国际金融期货交易所
		MARKET_XEurex = 260,                    //欧洲期货交易所
		
		//美洲地区
		MARKET_CME = 513,						//芝加哥商业交易所
		MARKET_CBOT = 514,						//芝加哥期货交易所
		MARKET_NYBOT = 515,						//纽约期货交易所
		MARKET_NYMEX_COMEX = 516,				//纽约商业交易所
		MARKET_ICE_CANOLA = 517,				//加拿大油菜籽期货交易所
		MARKET_eCBOT = 518,                     //芝加哥电子交易所
        MARKET_CBOE = 520,                      //芝加哥期权交易所

		//其他地区
		MARKET_SFE = 768,						//澳洲交易所
		MARKET_DME = 769,						//阿联酋迪拜商品交易所
		MARKET_DGCX = 770                       //迪拜黄金和商品交易所
	};

	//品种ID定义--------------------------------------------------
	enum Variety
	{
		VARIETY_ALL = 0,
		VARIETY_STOCK = 1,						//股票
		VARIETY_BOND,							//债券
		VARIETY_FUND,							//基金
		VARIETY_SPOT,							//现货
		VARIETY_MONEY_MARKET,					//货币市场工具 包括货币基金,回购,票据,短期债等等
		VARIETY_INDEX,							//指数
		VARIETY_FUTURE = 10,					//期货
		VARIETY_OPTION,							//期权
		VARIETY_WARRANT,						//权证
		VARIETY_STOCK_OPTION = 15				//个股期权
	};

	//股票分类--------------------------------------------------
	enum StockType
	{
		STOCK_ALL = 0,
		STOCK_COMMON = 1,						//普通股票
		STOCK_PREFERRED,						//优先股
	};

	//债券分类
	enum BondType
	{
		BOND_ALL = 0,
		BOND_NATIONAL,							//国债
		BOND_CORPORATE,							//企业债
		BOND_CONVERTIBLE,						//转债
		BOND_MUNICIPAL,							//地方债
		BOND_FINANCIAL							//金融债
	};

	//基金分类
	enum FundType
	{
		FUND_ALL = 0,
		FUND_CLOSED,							//封闭基金
		FUND_OPEN,								//开放基金 非ETF非LOF,可认购,申购,赎回
		FUND_GRADING,							//分级基金(子母基金都在这个里面)
		FUND_LOF,								//lof基金
		FUND_ETF								//etf基金
	};

	//现货分类
	enum SpotType
	{
		SPOT_ALL,
		SPOT_COMMODITY_IMME,					//商品现货-即期交易
		SPOT_NONLE_METAL_IMME,					//贵金属现货-即期交易(黄金，白银,?Z金，铂金)
		SPOT_COMMODITY_DELAY,					//商品现货 - 延期交易
		SPOT_NONLE_METAL_DELAY,					//贵金属现货 - 延期交易（T + D / T + N1)
		SPOT_COMMODITY_ACTUAL,					//贵金属现货实盘合约
		SPOT_NONLE_METAL_ACTUAL					//商品现货实盘合约
	};

	//货币市场
	enum MarketMoneyType
	{
		MARKET_MONEY_ALL,
		MARKET_MONEY_COLLATERALISED_REPO,			//质押式回购
		MARKET_MONEY_SHORT_TERM_BOND,				//短期债券
		MARKET_MONEY_BILL,							//票据
		MARKET_MONEY_BIG_DEPOSIT,					//大额存单
		MARKET_MONEY_MONETARY_FUND,					//货币基金
		MARKET_MONEY_BUYOUT_REPO,					//买断式回购
	};

	//指数
	enum IndexType
	{
		INDEX_ALL,
		INDEX_COMPOSITE,							//综合指数
		INDEX_INDUSTRY								//行业指数
	};

	//期货分类
	enum FutureType
	{
		FUTURE_ALL,
		FUTURE_STOCK_INDEX,							//股指期货
		FUTURE_COMMODITY,							//商品期货
		FUTURE_TREASURY_BOND,						//国债期货
		FUTURE_INTEREST_RATE,						//利率期货
		FUTURE_EXCHANGE								//汇率期货
	};

	//期权分类 call put标志放在属性表里面
	enum OptionType
	{
		OPTION_ALL,
		OPTION_INDEX,								//指数期权
		OPTION_ETF,									//ETF期权
		OPTION_BINARY,								//二元期权(牛证)
		OPTION_COMMODITY_FUTURE,					//商品期货期权
		OPTION_INTEREST_RATE,						//利率期权
		OPTION_EXCHANGE,							//汇率期权
	};

	//权证分类
	enum WarrantType
	{
		WARRANT_ALL,
		WARRANT_STOCK,								//股票认购权证
	};

	//货币分类
	enum Currency
	{
		CCYCNY = 1,		//人民币
		CCYUSD,		//美元
		CCYEUR,		//欧元
		CCYJPY,		//日元
		CCYGBP,		//英镑
		CCYRUB, 	//卢布
		CCYCHF,		//瑞士法郎
		CCYHKD,		//港币
		CCYAUD,		//澳元
		CCYKRW,		//韩元
		CCYTHB,		//泰铢
		CCYBRL,		//巴西雷亚尔
		CCYNZD,		//新西兰元
		CCYSGD,		//新加坡元
		CCYMYR,		//马来西亚林吉特
		CCYCAD,		//加元
	};
	
	//证券基本信息结构体 每天获取一次
	struct SecurityInfo
	{
		int64_t	ukey;									//uk码
		int32_t	market_id;
		int32_t	major_type;								//大类型
		int32_t	minor_type;								//小类型
		char	market_code[32];						//交易所标识600446 合约名称
		char	market_abbr[32];						//证券简称 中文
		char	chinese_name[128];  					//中文名称 证券简称
		char	english_name[128]; 						//英文名
		int32_t	list_date;								//上市时间
		int32_t	delist_date;							//退市时间
		int32_t	currency_id;							//结算和交易货币编码,用于货币交易的,也用于品种交易的货币.
		int32_t	jy_code; 	 							//JY  证券代码  InnerCode int32_t聚源数据库码
		char	wind_code[32];							//wind证券代码 s_info_windcode 'A14630.SZ' wind是字符类型
		char	input_code[64];						    //中文拼写 inputcode
		char    trading_time[64];                       //时间段[090000,113000],[130000,150000]

		int32_t	trading_day;							//交易日
		int32_t	pre_trading_day;						//上个交易日
		int64_t	upper_limit;							//涨停价格 *10000
		int64_t	lower_limit;							//跌停价铬 *10000
		int64_t	pre_close;								//昨收盘   *10000
		int64_t	pre_settlement;							//昨结算   *10000
		int64_t	pre_interest;							//昨持仓
		int64_t	pre_volume;								//上次交易总量
		int64_t	total_share;							//总股本，昨总持仓合约数．
		int64_t	float_share;							//流通股本
		int64_t	associate_code;							//关联编码 比如转债的关联股票 uk
		int64_t	exercise_price;							//行权价 期权行权价格 转债转股价格 *10000
		int32_t	contract_id;							//期货/期权有对应的合约编码 对照到contract表
		int32_t	min_order_size;							//最小单笔成交量 单位手 黄金交易所 Au99.95 1手 股票一般是1手
		int32_t	max_order_size;							//最大单笔成交量 单位手 黄金交易所 Au99.95 500手
		int32_t	lot_size;								//每手股数 股票使用
		int32_t	multiplier;								//合约乘数 股指期货每点300元 交易单位 商品期货1000克/手  期权合约乘数10000 期货期权使用
		int32_t	tick_size;								//*10000 股票0.01 商品期货最小变动价位 0.05元/克 股指期货0.2指数点 国债期货0.005元 期权0.0001元
		int32_t	last_delivery_date;						//最后交割日
		int32_t min_trading_margin;                     //最低交易保证金比例 7% 即7
		//int32_t	call_put;							//callput标志 除权标志 CALL 0 PUT 1
		//int32_t	ex_flag;							//除权标志 M 0 A 1 B2
		int32_t	share_arrive;							//证券到帐日期延时, 0 : T + 0, 1 : T + 1, 2 : T + 2
		int32_t	money_arrive;							//资金到帐日期延时, 0 : T + 0, 1 : T + 1, 2 : T + 2
		int32_t	share_avail;							//证券可用于交易的时间, 0 : T + 0, 1 : T + 1, 2 : T + 2
		int32_t	money_avail;							//资金可用于交易的时间, 0 : T + 0, 1 : T + 1, 2 : T + 2
		int32_t	state;									//品种状态，与大分类有关．是否ST, PT, 正常 0; ST 2; *ST 3; **ST 4; 暂停 5; 停止 6
		int32_t	board;									//板块 1主板 2中小板 3创业板 4三板 
	};
/*
	//证券基本信息结构体 每天获取一次
	struct SecurityInfo
	{
		int64_t	ukey;									//uk码
		int32_t	market_id;
		int32_t	major_type;								//大类型
		int32_t	minor_type;								//小类型
		int32_t	jy_code; 	 							//JY  证券代码  InnerCode int32_t聚源数据库码
		int32_t	list_date;								//上市时间
		int32_t	delist_date;							//退市时间
		int32_t	currency_id;							//结算和交易货币编码,用于货币交易的,也用于品种交易的货币.
		char	market_code[32];						//交易所标识600446 合约名称
		char	wind_code[32];							//wind证券代码 s_info_windcode 'A14630.SZ' wind是字符类型
		char	chinese_name[128];  					//中文名称 证券简称
		char	english_name[128]; 						//英文名
		char	chinese_abbr[32];						//证券简称 中文
		char	input_code[64];						    //中文拼写 inputcode

		int64_t	upper_limit;							//涨停价格 *10000
		int64_t	lower_limit;							//跌停价铬 *10000
		int64_t	pre_close;								//昨收盘   *10000
		int64_t	pre_settlement;							//昨结算   *10000
		int64_t	pre_interest;							//昨持仓
		int64_t	pre_volume;								//上次交易总量
		int64_t	exercise_price;							//行权价 期权行权价格 转债转股价格 *10000
		int64_t	total_volume;							//总股本，昨总持仓合约数．
		int64_t	float_volume;							//流通股本
		int64_t	associate_code;							//关联编码 比如转债的关联股票 uk
		int32_t	contract_id;							//期货/期权有对应的合约编码 对照到contract表
		int32_t	pre_trading_day;						//上个交易日
		int32_t	trading_day;							//交易日
		int32_t	min_order_size;							//最小单笔成交量 单位手 黄金交易所 Au99.95 1手 股票一般是1手
		int32_t	max_order_size;							//最大单笔成交量 单位手 黄金交易所 Au99.95 500手
		int32_t	lot_size;								//每手股数 股票使用
		int32_t	multiplier;								//合约乘数 股指期货每点300元 交易单位 商品期货1000克/手  期权合约乘数10000 期货期权使用
		int32_t	tick_size;								//10000 股票0.01 商品期货最小变动价位 0.05元/克 股指期货0.2指数点 国债期货0.005元 期权0.0001元
		int32_t	last_delivery_date;						//最后交割日
		int32_t	call_put;								//callput标志 除权标志 CALL 0 PUT 1
		int32_t	ex_flag;								//除权标志 M 0 A 1 B2
		int32_t	share_arrive;							//证券到帐日期延时, 0 : T + 0, 1 : T + 1, 2 : T + 2
		int32_t	money_arrive;							//资金到帐日期延时, 0 : T + 0, 1 : T + 1, 2 : T + 2
		int32_t	share_avail;							//证券可用于交易的时间, 0 : T + 0, 1 : T + 1, 2 : T + 2
		int32_t	money_avail;							//资金可用于交易的时间, 0 : T + 0, 1 : T + 1, 2 : T + 2
		int32_t	state;									//品种状态，与大分类有关．是否ST, PT, 正常 0; ST 2; *ST 3; **ST 4; 暂停 5; 停止 6
		int32_t	plate;									//板块 1主板 2中小板 3创业板 4三板 
	};


	struct Contract
	{
		int32_t     contract_id;										//合约编码 按上市时间
		int32_t		list_date;      									//上市日期, 首个合同交易日期 如果觉得不是很关心 标的的上市时间可以去掉
		int32_t		delist_date;										//下市日期, 摘牌日期, 当日还可以交易, 后期不行, 缺省是99999999
		char		contract_code[32];									//市场编码 CU
		char		contract_chname[128];								//标的中文名称 伦敦铜
		char		contract_enname[128];								//标的英文名称 CU
		char		pricen_unit[32];									//报价单位 元（人民币）/吨 报价方式
		char		max_fluctuation_limit[256];						    //每日价格最大波动限制 不超过上一交易日结算价±3%
		char		contract_month[256];								//合约交割月份 1～12月
		char		trading_time[256];									//交易时间 上午9 : 00－11 : 30 ，下午1 : 30－3 : 00和交易所规定的其他交易时间
		char		last_trading_date[64];								//最后交易日 合约交割月份的15日
		char		delivery_date[64];									//交割日期 最后交易日后连续五个工作日 或是最后交割日描述
		char		delivery_grade[512];								//交割品级
		char		delivery_points[64];								//交割地点
		char		min_trading_margin[512];							//最低交易保证金
		char		trading_fee[64];									//交易手续费
		char		delivery_methods[64];								//交割方式
		char		contract_desc[1024];    							//合约描述
	};
*/

	struct Contract
	{
		int32_t     contract_id;										//合约编码 按上市时间
		char		contract_code[32];									//市场编码 CU
		char		chinese_name[128];			     					//标的中文名称 伦敦铜
		char		english_name[128];				    				//标的英文名称 CU
		char        multiplier_desc[128];                               //5吨/手
		char        price_unit_desc[32];                                //报价单位 元（人民币）/吨 
		char        tick_size_desc[32];                                 //最小变动价位 5元/吨 
		char        max_fluctuation_limit_desc[256];                    //每日价格最大波动限制 不超过上一交易日结算价±3%
		char        contract_month_desc[256];                           //合约交割月份 1～12月
		char        trading_time_desc[256];                             //交易时间 上午9:00－11:30 ，下午1:30－3:00和交易所规定的其他交易时间
		char        last_trading_date_desc[64];                         //最后交易日 合约交割月份的15日
		char        delivery_date_desc[64];                             //交割日期 最后交易日后连续五个工作日 或是最后交割日描述
		char        delivery_grade_desc[512];                           //交割品级
		char        delivery_points_desc[64];                           //交割地点
		char        min_trading_margin_desc[512];                       //最低交易保证金
		char        trading_fee_desc[64];                               //交易手续费
		char        delivery_methods_desc[64];                          //交割方式
		int32_t		list_date;      									//上市日期, 首个合同交易日期 如果觉得不是很关心 标的的上市时间可以去掉
		int32_t		delist_date;										//下市日期, 摘牌日期, 当日还可以交易, 后期不行, 缺省是99999999
		int32_t     currency_id;                                        //交易货币
		char		contract_desc[1024];    							//合约描述
	};

	struct Component
	{
		int64_t component_id;                                           //1上证50 2 上证180 3沪深300 4etf成分股
		int64_t ukey;                                                   //ukey
		int32_t stock_amount;											//股票数量(股)
		int32_t cash_substitute_sign;                                   //现金替代标志 1 允许 2必须 3 ...
		double cash_substitute_proportion;                              //现金替代比例(%)
		double fixed_substitute_money;                                  //固定替代金额(元)
	};

	struct Market
	{
		int32_t market_id;       										//交易市场
		int32_t	currency_id;     										//交易所基础货币编码, 但期货合约币种可能与这个也不同
		int32_t time_zone;                                              //时区 中国为8 东为正，西为负
		char	brief_code[12];		   									//市场简码, 比如CZC, SHF, SZ
		char    trading_time[64];                                       //时间段[090000,113000]所有品种汇总
		char	chinese_name[128];   									//市场中文名称
		char	english_name[128];   									//标的英文名称
	};

	struct Exrights
	{
		int64_t ukey;                                                   //ukey
		int32_t exdividate;                                             //除权分红日期
		double bonus_ratio;                                             //送股 10送5=1.5
		double increase_ratio;                                          //转增 10送5=1.5
		double rationed_ratio;                                          //配股 10配2.8=1.28
		double trans_ratio;                                             //对价 10对价4.5股=1.45
		double trans_price;                                             //对价 送钱金额 元/股
		double exprice;                                                 //分红派息金额 元/股
		double former_complex_rights;                                   //前复权系数
		double after_complex_rights;                                    //后复权系数
	};

	struct Feerate
	{
		int64_t ukey;                                                   //ukey
		int32_t directive;                                              //指令 1 开仓 2平仓  3平今仓 4 平昨仓
		double fee_by_amount;                                           //按成交金额
		double fee_by_qty;                                              //按成交量
		double minimum_money;                                           //最低收费金额如最低5元
	};

	class SecurityMaster
	{
	public:
		//以数据库的方式加载secu-marster 选择加载的市场，可以多次open前后覆盖 指定日期只得到某天 默认0 代表全部最近一天的
		int32_t UKOpen(const std::set<int32_t>& market_set, const std::string& url, const std::string& user, const std::string& pass, const std::string& data_base, const int date = 0);

		//以文件的方式加载secu-marster 3个.csv文件
		int32_t UKOpen(const std::string& file_dir);
		
		//请求接口
		int32_t UKOpen(const std::string& ip, const uint32_t& port, const int& date);

		//数据库down到本地文件
		int32_t UKDownload(const std::string& file_dir);

		//通过uk码确定信息
		//0 成功 其他失败
		int32_t GetBaseInfo(const int64_t& ukey, SecurityInfo& info);
		//NULL 失败 其他成功
		const SecurityInfo* GetBaseInfo(const int64_t& ukey);

		//返回market_id
		int32_t MarketId(const int64_t& ukey);
		//返回major_type
		int32_t MajorType(const int64_t& ukey);
		//返回minor_type
		int32_t MinorType(const int64_t& ukey);
		//返回market_code
		const char* MarketCode(const int64_t& ukey);
		//返回market_abbr
		const char* MarketAbbr(const int64_t& ukey);
		//返回chinese_name
		const char* ChineseName(const int64_t& ukey);
		//返回english_name
		const char* EnglishName(const int64_t& ukey);
		//返回list_date
		int32_t ListDate(const int64_t& ukey);
		//返回delist_date
		int32_t DelistDate(const int64_t& ukey);
		//返回currency_id
		int32_t CurrencyId(const int64_t& ukey);
		//返回jy_code
		int32_t JyCode(const int64_t& ukey);
		//返回wind_code
		const char* WindCode(const int64_t& ukey);
		//返回input_code
		const char* InputCode(const int64_t& ukey);
		//返回trading_time
		const char* TradingTime(const int64_t& ukey);
		//返回trading_day
		int32_t TradingDay(const int64_t& ukey);
		//返回pre_trading_day
		int32_t PreTradingDay(const int64_t& ukey);
		//返回upper_limit
		int64_t UpperLimit(const int64_t& ukey);
		//返回lower_limit
		int64_t LowerLimit(const int64_t& ukey);
		//返回pre_close
		int64_t PreClose(const int64_t& ukey);
		//返回pre_settlement
		int64_t PreSettlement(const int64_t& ukey);
		//返回pre_interest
		int64_t PreInterest(const int64_t& ukey);
		//返回pre_volume
		int64_t PreVolume(const int64_t& ukey);
		//返回total_share
		int64_t TotalShare(const int64_t& ukey);
		//返回float_share
		int64_t FloatShare(const int64_t& ukey);
		//返回associate_code
		int64_t AssociateCode(const int64_t& ukey);
		//返回exercise_price
		int64_t ExercisePrice(const int64_t& ukey);
		//返回contract_id
		int32_t ContractId(const int64_t& ukey);
		//返回min_order_size
		int64_t MinOrderSize(const int64_t& ukey);
		//返回max_order_size
		int64_t MaxOrderSize(const int64_t& ukey);
		//返回lot_size
		int32_t LotSize(const int64_t& ukey);
		//返回multiplier
		int32_t Multiplier(const int64_t& ukey);
		//返回tick_size
		int32_t TickSize(const int64_t& ukey);
		//返回last_delivery_date
		int32_t LastDeliveryDate(const int64_t& ukey);
		//返回min_trading_margin
		int32_t	MinTradingMargin(const int64_t& ukey);
		//返回share_arrive
		int32_t ShareArrive(const int64_t& ukey);
		//返回money_arrive
		int32_t MoneyArrive(const int64_t& ukey);
		//返回share_avail
		int32_t ShareAvail(const int64_t& ukey);
		//返回money_avail
		int32_t MoneyAvail(const int64_t& ukey);
		//返回state
		int32_t State(const int64_t& ukey);
		//返回board
		int32_t Board(const int64_t& ukey);
		//下单单位 股票等LotSize*MinOrderSize 期货等Multiplier
		//int32_t OrderUnit(const int64_t& ukey);

		
		/*
			通过交易所编码和市场id确定信息
		*/

		//返回值 0 成功 其他失败
		int32_t GetBaseInfo(const int32_t& market_id, const std::string& market_code, SecurityInfo& info);
		//返回NULL 失败 其他成功
		const SecurityInfo* GetBaseInfo(const int32_t& market_id, const std::string& market_code);
		//返回ukey -1 没有找到
		int64_t Ukey(const int32_t& market_id, const std::string& market_code);


		/*
			marketcode确定ukid
		*/ 

		//结果返回匹配个数
		int32_t GetBaseInfo(const std::string& market_code, std::vector<SecurityInfo>& info);
		//返回结果
		std::vector<SecurityInfo*> GetBaseInfo(const std::string& market_code);
		//返回ukey
		std::vector<int64_t> MultiUkey(const std::string& market_code);
		

		/*
			通过windcode确定信息 大小写都可以
		*/

		//返回值 0 成功 其他失败
		int32_t WindCodeGetBaseInfo(const std::string& wind_code, SecurityInfo& info);
		//返回NULL 失败 其他成功
		const SecurityInfo* WindCodeGetBaseInfo(const std::string& wind_code);
		//返回ukey -1 没有找到
		int64_t UkeyFromWind(const std::string& wind_code);



		/*
			通过jyinnercode确定信息
		*/

		//返回值 0 成功 其他失败
		int32_t InnerCodeGetBaseInfo(const int32_t& inner_code, SecurityInfo& info);
		//返回NULL 失败 其他成功
		const SecurityInfo* InnerCodeGetBaseInfo(const int32_t& inner_code);
		//返回ukey -1 没有找到
		int64_t UkeyFromJy(const int32_t& inner_code);
		


		/*
			获取期货期权标的信息
		*/
		//返回值 0 成功 其他失败
		int32_t GetContract(const int32_t& contract_id, Contract& contract);
		//返回NULL 失败 其他成功
		const Contract* GetContract(const int32_t& contract_id);

		/*
			根据ukid 获取合约信息
		*/
		//返回值 0 成功 其他失败
		int32_t GetContract(const int64_t& ukey, Contract& contract);
		//返回NULL 失败 其他成功
		const Contract* GetContract(const int64_t& ukey);


				
		//获取市场信息
		int32_t GetMarket(const int32_t& market_id, Market& market);
		//返回市场信息 NULL为失败
		const Market* GetMarket(const int32_t& market_id);

		//根据ukid 获取市场信息
		int32_t GetMarket(const int64_t& ukey, Market& market);
		//返回市场信息 NULL为失败
		const Market* GetMarket(const int64_t& ukey);

		/*
			获取 指数成分 ETF成分股
		*/
		//
		std::vector<int64_t> GetComponent(const int64_t& component_id);
		
		//是否在某个成份股 返回0 代表有并放入第三个参数 其他没有
		int GetComponent(const int64_t& component_id, const int64_t& key, Component& com);
		//NULL没有该成分股
		const Component* GetComponent(const int64_t& component_id, const int64_t& key);

		//返回费率
		//const Feerate* GetFeerate(const int64_t& ukey);

	};

}
#pragma pack(pop)
#endif
// __HONGKING_UK_API_
