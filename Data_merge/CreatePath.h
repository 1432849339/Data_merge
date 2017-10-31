#ifndef  __CREATEPATH_H_
#define  __CREATEPATH_H_

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

using namespace std;

string itostr(int64_t num);
bool CreatePath(string Path);
bool mkdir_path(string BasePath);

#endif //  __CREATEPATH_H_


