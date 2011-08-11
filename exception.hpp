/// exception.hpp -- error codes

/// Copyright (c) 2011 Vobile. All rights reserved.
/// Author: Zhang Yichao <zhang_yichao@vobile.cn>
/// Created: 2011-08-03
///

#ifndef INCLUDED_EXCEPTION_HPP
#define INCLUDED_EXCEPTION_HPP

#include <stdexcept>
#include <string>

enum error
{
    success = 0,

    // protocol errors often means you don't send the correct number of
    // zmq frames in a request
    bad_proto = 0x1,
    bad_req = 0x2,
    bad_txn = 0x3,
    bad_arg = 0x4,
    // txn must be run by only one caller, 'coz we send timeout notifications
    // to the caller
    bad_caller = 0x5,

    // db logic error
    db_dup = 0x11,
    db_noref = 0x12,
    db_reffed = 0x13,

    db_stmt = 0x21,             // can retry stmt, txn is safe
    db_txn = 0x22,              // do not retry, whole txn is doomed
    // to notify the txn initiater that its txn has timed out
    // sending another req with the same txn id may produce no response at all
    txn_timeout = 0x23,

    not_support = 0x31,
};

std::string err_to_str (error e);

class coded_error : public std::runtime_error
{
public:
    coded_error (error e, const std::string &m = "")
        : std::runtime_error (m.empty () ? err_to_str (e) : m), err_ (e) {}
    error code () const {return err_;}
private:
    error err_;
};

#endif // INCLUDEDd_EXCEPTION_HPP
