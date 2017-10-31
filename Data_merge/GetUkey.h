#ifndef _GETUKEY_H__
#define _GETUKEY_H__ 
#include <string.h>
#include <set>
#include <iostream>
#include <string>
#include <map>
#include <algorithm>
#include "ukey.h"

using namespace std;
using namespace chronos;

bool Init_ukdb09(const std::set<std::string>& market_set, bool use_file = false);
int64_t	GetUkey(std::string market, std::string code);

#endif