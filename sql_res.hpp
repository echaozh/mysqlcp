/// sql_res.hpp -- sql results decls

/// Author: Zhang Yichao <echaozh@gmail.com>
/// Created: 2011-08-03
///

#ifndef INCLUDED_SQL_RES_HPP
#define INCLUDED_SQL_RES_HPP

#include "exception.hpp"
#include "sql_stmt.hpp"

#include <cppzmq.hpp>

#include <string>

struct sql_res
{
    sql_res () : empty (true), id (0), err (success), txn_seq (0) {}
    sql_res (sql_res &&rhs)
        : empty (rhs.empty), addr (std::move (rhs.addr)), id (rhs.id),
          err (rhs.err), msg (rhs.msg), txn_seq (rhs.txn_seq),
          res (std::move (rhs.res)) {}
    sql_res (cppzmq::packet_t &&a, size_t txn, error e,
             const std::string &m = "")
        : empty (false), addr (a), id (0), err (e), msg (m), txn_seq (txn)
        {
            if (msg.empty ())
                msg = err_to_str (err);
        }
    sql_res (sql_stmt &&stmt, const std::string &&r = "")
        : empty (false), addr (std::move (stmt.addr)), id (stmt.id),
          err (stmt.err), msg (stmt.msg), txn_seq (stmt.txn_seq), res (r)
        {
            if (msg.empty ())
                msg = err_to_str (err);
        }
    sql_res (sql_stmt &&stmt, error e, const std::string &m = "")
        : empty (false), addr (std::move (stmt.addr)), id (stmt.id), err (e),
          msg (m), txn_seq (stmt.txn_seq)
        {
            if (msg.empty ())
                msg = err_to_str (err);
        }
    sql_res &operator = (sql_res &&rhs)
        {
            empty = rhs.empty;
            addr = std::move (rhs.addr);
            id = rhs.id;
            err = rhs.err;
            msg = rhs.msg;
            txn_seq = rhs.txn_seq;
            res = std::move (rhs.res);
            return *this;
        }

    bool empty;
    cppzmq::packet_t addr;
    size_t id;
    error err;
    std::string msg;
    size_t txn_seq;
    std::string res;
};

#endif // INCLUDED_SQL_RES_HPP
