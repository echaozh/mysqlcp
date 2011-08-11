/// sql_stmt.hpp -- sql statement decls

/// Copyright (c) 2011 Vobile. All rights reserved.
/// Author: Zhang Yichao <zhang_yichao@vobile.cn>
/// Created: 2011-08-03
///

#ifndef INCLUDED_SQL_STMT_HPP
#define INCLUDED_SQL_STMT_HPP

#include "exception.hpp"
#include "mysql_stmt.hpp"

#include <json/json.h>
#include <cppzmq.hpp>

#include <string>
#include <tr1/memory>
#include <tr1/unordered_map>

struct mysql_stmt;
struct sql_stmt
{
    sql_stmt (cppzmq::packet_t &&a, const cppzmq::message_t &sql,
              const std::tr1::unordered_map<std::string,
                                            std::tr1::shared_ptr<mysql_stmt>
                                            > &stmts);
    sql_stmt (sql_stmt &&rhs)
        : addr (std::move (rhs.addr)), id (rhs.id), err (rhs.err),
          msg (std::move (rhs.msg)), txn_seq (rhs.txn_seq),
          builtin (rhs.builtin), stmt (rhs.stmt)
        {std::swap (params, rhs.params);}
    ~sql_stmt () {if (params) json_object_put (params);}
    bool begins_txn () const {return builtin == begin;}
    bool ends_txn () const {return builtin == commit || builtin == rollback;}

    mutable cppzmq::packet_t addr;
    size_t id;
    error err;
    std::string msg;
    size_t txn_seq;
    enum builtin_stmt {none, begin, commit, rollback} builtin;
    std::tr1::shared_ptr<mysql_stmt> stmt;
    struct json_object *params;
};

#endif // INCLUDED_SQL_STMT_HPP
