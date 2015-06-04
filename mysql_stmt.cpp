/// mysql_stmt.cpp -- mysql statement wrapper impl

/// Author: Zhang Yichao <echaozh@gmail.com>
/// Created: 2011-08-05
///

#include "exception.hpp"
#include "mysql_stmt.hpp"

#include <vconf/vconf.h>

#include <cstdlib>
#include <iostream>
#include <stdexcept>

using namespace std;

MYSQL_STMT *mysql_stmt::prepare (MYSQL *conn) const
{
    MYSQL_STMT *ps = mysql_stmt_init (conn);
    if (!ps)
        throw bad_alloc ();
    if (mysql_stmt_prepare (ps, sql.data (), sql.size ())) {
        mysql_stmt_close (ps);
        throw coded_error (db_stmt, mysql_stmt_error (ps));
    }
    return ps;
}

bind_type mysql_stmt::translate_type (MYSQL_FIELD *field)
{
    switch (field->type) {
    case MYSQL_TYPE_NULL:
        return null;
    case MYSQL_TYPE_TINY: case MYSQL_TYPE_SHORT: case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_INT24: case MYSQL_TYPE_LONGLONG:
        return field->flags & UNSIGNED_FLAG ? unsigned_int : integer;
    case MYSQL_TYPE_FLOAT: case MYSQL_TYPE_DOUBLE:
        return floating_point;
    case MYSQL_TYPE_STRING: case MYSQL_TYPE_VAR_STRING: case MYSQL_TYPE_ENUM:
    case MYSQL_TYPE_SET: case MYSQL_TYPE_DECIMAL: case MYSQL_TYPE_NEWDECIMAL:
        return text;
    case MYSQL_TYPE_BLOB:
        return binary;
    case MYSQL_TYPE_DATE: case MYSQL_TYPE_TIME: case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_TIMESTAMP:
        return timestamp;
    default:
        cerr << file << ":" << lineno << ": " << name
             << ": unsupported column type in results: " << endl << sql << endl
             << flush;
        throw runtime_error ("");
    }
}

static MYSQL *reconnect (const string &host, unsigned short port,
                         const string &user, const string &password,
                         const string &db, size_t timeout)
{
    MYSQL *conn = mysql_init (0);
    if (!conn)
        throw bad_alloc ();

    if (mysql_options (conn, MYSQL_OPT_CONNECT_TIMEOUT, (char *) &timeout)
        || mysql_options (conn, MYSQL_OPT_READ_TIMEOUT, (char *) &timeout)
        || mysql_options (conn, MYSQL_OPT_WRITE_TIMEOUT, (char *) &timeout)) {
        cerr << "failed to configure mysql connection, cannot proceed" << endl
             << flush;
        throw runtime_error ("");
    }
    if (!mysql_real_connect (conn, host.c_str (), user.c_str (),
                             password.c_str (), db.c_str (), port, 0,
                             CLIENT_IGNORE_SIGPIPE)) {
        cerr << "failed to connect to mysql server, cannot proceed" << endl
             << flush;
        throw runtime_error ("");
    }

    return conn;
}

void mysql_stmt::init_results (MYSQL *&conn, const string &host,
                               unsigned short port, const string &user,
                               const string &password, const string &db,
                               size_t timeout)
{
    if (insert_id)
        return;

    for (size_t i = 0; i < 3; ++i) {
        MYSQL_STMT *ps = 0;
        if (!conn)
            conn = reconnect (host, port, user, password, db, timeout);

        try {
            ps = prepare (conn);
            MYSQL_RES *res = mysql_stmt_result_metadata (ps);
            if (!res && mysql_stmt_errno (ps))
                throw runtime_error (mysql_stmt_error (ps));
            size_t fc = res ? mysql_num_fields (res) : 0;
            if (fc) {
                is_query = true;
                results.resize (fc);
                for (size_t i = 0; i < fc; ++i) {
                    results[i] = translate_type (
                        mysql_fetch_field_direct (res, i));
                }
            } else
                is_query = false;

            mysql_free_result (res);
            mysql_stmt_close (ps);
            break;
        } catch (const exception &e) {
            cerr << file << ":" << lineno << ": " << name
                 << ": failed to init result info due to: " << e.what ()
                 << "; sql: " << endl << sql << endl << flush;

            if (ps)
                mysql_stmt_close (ps);

            mysql_close (conn);
            conn = reconnect (host, port, user, password, db, timeout);
        }
    }
}

