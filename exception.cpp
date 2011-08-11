/// exception.cpp -- exception impl

/// Copyright (c) 2011 Vobile. All rights reserved.
/// Author: Zhang Yichao <zhang_yichao@vobile.cn>
/// Created: 2011-08-03
///

#include "exception.hpp"

using namespace std;

string err_to_str (error e)
{
    switch (e) {
    case success: return "success";
    case bad_proto: return "protocol error";
    case bad_req: return "bad request";
    case bad_txn: return "unknown transaction, perhaps it timed out earlier";
    case bad_arg: return "bad argument for sql statement";
    case bad_caller: return "transaction was initiated by another caller";

    case db_dup: return "duplicate key when inserting";
    case db_noref: return "foreign reference not found when inserting/updating";
    case db_reffed: return "key is referenced, cannot delete";

    case db_stmt: return "statement execution failed, you may retry";
    case db_txn: return "statement execution failed, transaction is doomed";
    case txn_timeout: return "transaction has timed out, do not continue";

    case not_support: return "statement to execute is not supported";

    default: return "unknown error";
    }
}
