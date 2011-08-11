/// mysql_stmt.hpp -- mysql statement decls

/// Copyright (c) 2011 Vobile. All rights reserved.
/// Author: Zhang Yichao <zhang_yichao@vobile.cn>
/// Created: 2011-08-03
///

#ifndef INCLUDED_MYSQL_STMT_HPP
#define INCLUDED_MYSQL_STMT_HPP

#include <mysql/mysql.h>

#include <deque>
#include <string>

struct vconf_url;

enum bind_type
{
    null, integer, unsigned_int, floating_point, text, binary, timestamp,
};

struct mysql_stmt
{
    mysql_stmt (const std::string &n, const std::string &s, bool i,
                const std::string &f, size_t l)
        : name (n), sql (s), insert_id (i), file (f), lineno (l),
          is_query (true) {}
    MYSQL_STMT *prepare (MYSQL *conn) const;
    void init_results (MYSQL *&conn, const std::string &host,
                       unsigned short port, const std::string &user,
                       const std::string &password, const std::string &db,
                       size_t timeout);

    std::string name;
    std::string sql;
    bool insert_id;

    std::string file;
    size_t lineno;

    bool is_query;
    std::deque<bind_type> results;

private:
    bind_type translate_type (MYSQL_FIELD *field);
};

#endif // INCLUDED_MYSQL_STMT_HPP
