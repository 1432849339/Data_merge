#include "CreatePath.h"



string itostr(int64_t num)
{
	char num_str[32];
	sprintf(num_str, "%ld", num);
	return string(num_str);
}

bool CreatePath(string Path)
{
	if (access(Path.c_str(), 0) == -1)
	{
		if (mkdir(Path.c_str(), 0777))
		{
			cout << "make errorly" << __LINE__ << endl;
			return false;
		}
	}
	return true;
}

bool mkdir_path(string BasePath)
{
	if (access(BasePath.c_str(), F_OK) == 0)
	{
		return true;
	}
	else
	{
		string::size_type pos = 1;
		while ((pos = BasePath.find('/', pos)) != string::npos)
		{
			string SubStr = BasePath.substr(0, pos);
			if (access(SubStr.c_str(), F_OK) == -1)
			{
				if (mkdir(SubStr.c_str(), 0777))
				{
					if (access(SubStr.c_str(), F_OK) == -1)
					{
						return false;
					}
				}
			}
			pos++;
		}
		return true;
	}
}