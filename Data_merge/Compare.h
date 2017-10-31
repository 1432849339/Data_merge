#include "cfg.h"


template<typename T>
struct compare_request
{
	bool operator()(T* t1, T* t2)
	{
		return ((t1->timeus) < (t2->timeus));
	}
};

template<>
struct compare_request<Transaction>
{
	bool operator()(Transaction* t1, Transaction* t2)
	{
		return (t1->index) < (t2->index);
	}
};

template<>
struct compare_request<Order>
{
	bool operator()(Order* t1, Order* t2)
	{
		return (t1->index) < (t2->index);
	}
};

template<typename T>
struct compare_quote
{
	bool operator()(T* t1, T* t2)
	{
		if (t1->timeus == t2->timeus)
		{
			return ((t1->ukey) < (t2->ukey));
		}
		else
		{
			return ((t1->timeus) < (t2->timeus));
		}	
	}
};

template<>
struct compare_quote<Transaction>
{
	bool operator()(Transaction* t1, Transaction* t2)
	{
		if (t1->timeus == t2->timeus)
		{
			if (t1->index == t2->index)
			{
				return ((t1->ukey) < (t2->ukey));
			}
			else
			{
				return (t1->index) < (t2->index);
			}
		}
		else
		{
			return (t1->timeus) < (t2->timeus);
		}	
	}
};

template<>
struct compare_quote<Order>
{
	bool operator()(Order* t1, Order* t2)
	{
		if (t1->timeus == t2->timeus)
		{
			if (t1->index == t2->index)
			{
				return ((t1->ukey) < (t2->ukey));
			}
			else
			{
				return (t1->index) < (t2->index);
			}
		}
		else
		{
			return (t1->timeus) < (t2->timeus);
		}
	}
};