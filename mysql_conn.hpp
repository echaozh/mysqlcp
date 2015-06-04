/// mysql_conn.hpp -- mysql connection wrapper decls

/// Author: Zhang Yichao <echaozh@gmail.com>
/// Created: 2011-08-03
///

#ifndef INCLUDED_MYSQL_CONN_HPP
#define INCLUDED_MYSQL_CONN_HPP

#include "sql_res.hpp"

#include <mysql/mysql.h>

#include <string>
#include <tr1/unordered_map>

struct sql_stmt;
class mysql_conn
{
public:
    mysql_conn (const std::string &host, unsigned short port,
                const std::string &user, const std::string &password,
                const std::string &db, size_t timeout)
        : host_ (host), port_ (port), user_ (user), password_ (password),
          db_ (db), timeout_ (timeout), conn_ (0) {}
    sql_res execute (sql_stmt &&stmt);
    void rollback ();
    void close ();

private:
    void connect ();
    sql_res real_exec (sql_stmt &&stmt);

private:
    std::string host_;
    unsigned short port_;
    std::string user_;
    std::string password_;
    std::string db_;
    size_t timeout_;

private:
    MYSQL *conn_;
    std::tr1::unordered_map<std::string, MYSQL_STMT *> stmts_;
};

#endif // INCLUDED_MYSQL_CONN_HPP
