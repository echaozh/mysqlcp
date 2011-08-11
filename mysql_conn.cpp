/// mysql_conn.cpp -- mysql connection wrapper impl

/// Copyright (c) 2011 Vobile. All rights reserved.
/// Author: Zhang Yichao <zhang_yichao@vobile.cn>
/// Created: 2011-08-03
///

#include "mysql_conn.hpp"

#include <mysql/errmsg.h>
#include <mysql/mysqld_error.h>

#include <boost/lexical_cast.hpp>

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <sstream>
#include <vector>

#include <iostream>

using namespace std;
using namespace boost;

void mysql_conn::close ()
{
    if (conn_) {
        tr1::unordered_map<string, MYSQL_STMT *>::iterator it;
        for (it = stmts_.begin (); it != stmts_.end (); ++it)
            mysql_stmt_close (it->second);
        stmts_.clear ();
        mysql_close (conn_);
        conn_ = 0;
    }
}

void mysql_conn::rollback ()
{
    if (!conn_)
        return;

    if (mysql_rollback (conn_) || mysql_autocommit (conn_, 1))
        close ();
}

static void throw_db_err (unsigned err, const string &msg)
{
    switch (err) {
    case 0:                 // no error
        return;

    case ER_DUP_KEY: case ER_DUP_ENTRY:
        throw coded_error (db_dup);

    case ER_NO_REFERENCED_ROW: case ER_NO_REFERENCED_ROW_2:
        throw coded_error (db_noref);

    case ER_ROW_IS_REFERENCED: case ER_ROW_IS_REFERENCED_2:
        throw coded_error (db_reffed);

    case CR_SERVER_LOST:
        throw coded_error (db_txn, "lost connection to mysql server");
    case CR_SERVER_GONE_ERROR:
        throw coded_error (db_txn, "mysql server gone");
        // case CR_CONNECTION_ERROR:
        //     throw database_error ("failed to connect to mysql server");
        // case CR_CONN_HOST_ERROR:
        //     throw database_error ("");
    default:
	    throw coded_error (db_stmt, string ("failed to execute statement: ")
                           + msg);
    }
}

static void checked_call (my_bool ret, MYSQL *conn)
{
    if (ret)
        throw_db_err (mysql_errno (conn), mysql_error (conn));
}

static void checked_call (my_bool ret, MYSQL_STMT *ps)
{
    if (ret)
        throw_db_err (mysql_stmt_errno (ps), mysql_stmt_error (ps));
}

static bind_type parse_type (const string &s)
{
    if (s == "long")
        return integer;
    else if (s == "unsigned")
        return unsigned_int;
    else if (s == "timestamp")
        return timestamp;
    else
        throw coded_error (bad_arg, "unrecognized parameter type");
}

static bind_type param_type (struct json_object *param)
{
    switch (json_object_get_type (param)) {
    case json_type_null:
        return null;
    case json_type_int:
        return integer;
    case json_type_double:
        return floating_point;
    case json_type_array:
        if (json_object_array_length (param) != 2)
            return binary;
        if (json_object_is_type (json_object_array_get_idx (param, 0),
                                 json_type_string)
            && json_object_is_type (json_object_array_get_idx (param, 1),
                                    json_type_string)) {
            return parse_type (json_object_get_string (
                                   json_object_array_get_idx (param, 0)));
        } else
            return binary;
    case json_type_string:
        return text;
    default:
        throw coded_error (bad_arg, "unsupported parameter type");
    }
}

static unsigned parse_bidigit (const char *s)
{
    assert (isdigit (s[0]) && isdigit (s[1]));
    return (s[0] - '0') * 10 + s[1] - '0';
}

static MYSQL_TIME *parse_time (const char *s)
{
    static const char format[] = "0000-00-00T00:00:00";
    if (strlen (s) != sizeof (format) - 1
        || (s[4] != format[4] && s[7] != format[7] && s[10] != format[10]
            && s[13] != format[13] && s[16] != format[16]))
        throw coded_error (bad_arg, "bad parameter value");
    unsigned year, month, day, hour, min, sec;
    year = parse_bidigit (s) * 100 + parse_bidigit (&s[2]);
    month = parse_bidigit (&s[5]);
    day = parse_bidigit (&s[5]);
    hour = parse_bidigit (&s[5]);
    min = parse_bidigit (&s[5]);
    sec = parse_bidigit (&s[5]);
    MYSQL_TIME *time = (MYSQL_TIME *) calloc (1, sizeof (*time));
    time->year = year;
    time->month = month;
    time->day = day;
    time->hour = hour;
    time->minute = min;
    time->second = sec;
    return time;
}

static void *param_value (struct json_object *param, bind_type type,
                          unsigned long *len = 0)
{
    switch (json_object_get_type (param)) {
    case json_type_null:
        return 0;
    case json_type_int:
        if (true) {
            int64_t *ret = (int64_t *) malloc (8);
            *ret = json_object_get_int (param);
            return ret;
        }
    case json_type_double:
        if (true) {
            double *ret = (double *) malloc (sizeof (double));
            *ret = json_object_get_double (param);
            return ret;
        }
    case json_type_array:
        if (true) {
            size_t l = json_object_array_length (param);
            if (type == binary) {
                for (size_t i = 0; i < l; ++i) {
                    if (!json_object_is_type (
                            json_object_array_get_idx (param, i),
                            json_type_int)) {
                        throw coded_error (bad_arg,
                                           "unrecognized parameter type");
                    }
                }
                unsigned char *buf = (unsigned char *) malloc (l);
                for (size_t i = 0; i < l; ++i) {
                    buf[i] = json_object_get_int (
                        json_object_array_get_idx (param, i));
                }
                if (len)
                    *len = l;
                return buf;
            } else {
                const char *s = json_object_get_string (
                    json_object_array_get_idx (param, 1));
                if (!*s)
                    throw coded_error (bad_arg, "bad parameter value");
                switch (type) {
                case integer:
                    if (true) {
                        char *p;
                        int64_t n = strtoll (s, &p, 0);
                        while (*p && isspace (*p))
                            ++p;
                        if (*p)
                            throw coded_error (bad_arg, "bad parameter value");
                        int64_t *ret = (int64_t *) malloc (8);
                        *ret = n;
                        return ret;
                    }
                case unsigned_int:
                    if (true) {
                        char *p;
                        uint64_t n = strtoull (s, &p, 0);
                        while (*p && isspace (*p))
                            ++p;
                        if (*p)
                            throw coded_error (bad_arg, "bad parameter value");
                        uint64_t *ret = (uint64_t *) malloc (8);
                        *ret = n;
                        return ret;
                    }
                case timestamp:
                    return parse_time (s);
                default:
                    assert (0);
                    throw coded_error (bad_arg, "unsupported parameter type");
                }
            }
        }
    case json_type_string:
        if (true) {
            char *s = strdup (json_object_get_string (param));
            if (len)
                *len = strlen (s);
            return s;
        }
    default:
        assert (0);
        throw coded_error (bad_arg, "unsupported parameter type");
    }
}

static void bind_param (MYSQL_BIND *bd, struct json_object *param)
{
    memset (bd, 0, sizeof (*bd));
    bind_type type = param_type (param);
    switch (type) {
    case null:
        bd->buffer_type = MYSQL_TYPE_NULL;
        break;
    case integer: case unsigned_int:
        bd->buffer_type = MYSQL_TYPE_LONGLONG;
        bd->buffer = malloc (8);
        bd->buffer = param_value (param, type);
        bd->buffer_length = 8;
        bd->is_unsigned = type == unsigned_int;
        break;
    case floating_point:
        bd->buffer_type = MYSQL_TYPE_DOUBLE;
        bd->buffer = malloc (sizeof (double));
        bd->buffer = param_value (param, type);
        bd->buffer_length = sizeof (double);
        break;
    case text:
        bd->buffer_type = MYSQL_TYPE_STRING;
        bd->buffer = param_value (param, type, &bd->buffer_length);
        bd->length = new unsigned long;
        *bd->length = bd->buffer_length;
        break;
    case binary:
        bd->buffer_type = MYSQL_TYPE_BLOB;
        bd->buffer = param_value (param, type, &bd->buffer_length);
        bd->length = new unsigned long;
        *bd->length = bd->buffer_length;
        break;
    case timestamp:
        bd->buffer_type = MYSQL_TYPE_TIMESTAMP;
        bd->buffer = param_value (param, type);
        bd->buffer_length = sizeof (MYSQL_TIME);
        break;
    default:
        assert (0);
        throw coded_error (bad_arg, "unsupported parameter type");
    }
}

static void bind_res (MYSQL_BIND *bd, bind_type type)
{
    memset (bd, 0, sizeof (*bd));
    switch (type) {
    case null:
        bd->buffer_type = MYSQL_TYPE_NULL;
        break;
    case integer: case unsigned_int:
        bd->buffer_type = MYSQL_TYPE_LONGLONG;
        bd->buffer = new int64_t;
        bd->buffer_length = 8;
        bd->is_unsigned = type == unsigned_int;
        bd->is_null = new my_bool;
        break;
    case floating_point:
        bd->buffer_type = MYSQL_TYPE_DOUBLE;
        bd->buffer = new double;
        bd->buffer_length = sizeof (double);
        bd->is_null = new my_bool;
        break;
    case text:
        bd->buffer_type = MYSQL_TYPE_STRING;
        bd->buffer = 0;
        bd->buffer_length = 0;
        bd->length = new unsigned long;
        bd->is_null = new my_bool;
        break;
    case binary:
        bd->buffer_type = MYSQL_TYPE_BLOB;
        bd->buffer = 0;
        bd->buffer_length = 0;
        bd->length = new unsigned long;
        bd->is_null = new my_bool;
        break;
    case timestamp:
        bd->buffer_type = MYSQL_TYPE_TIMESTAMP;
        bd->buffer = new MYSQL_TIME;
        bd->buffer_length = sizeof (MYSQL_TIME);
        bd->is_null = new my_bool;
        break;
    default:
        assert (0);
        throw coded_error (not_support, "unsupported column type in results");
    }
}

static void print_json_str (ostream &rs, const char *s, size_t len)
{
    struct json_object *o = json_object_new_string_len (s, len);
    if (!o)
        throw bad_alloc ();
    rs << json_object_to_json_string (o);
    json_object_put (o);
}

static void gen_row_res (ostream &rs, MYSQL_STMT *ps, vector<MYSQL_BIND> &binds)
{
    for (size_t i = 0; i < binds.size (); ++i) {
        if (i)
            rs << ",";
        if (*binds[i].is_null) {
            rs << "null";
            continue;
        }

        void *&buf = binds[i].buffer;
        size_t len = binds[i].length ? *binds[i].length : 0;
        if (len > binds[i].buffer_length) {
            if (binds[i].buffer) {
                assert (binds[i].buffer_type == MYSQL_TYPE_STRING
                        || binds[i].buffer_type == MYSQL_TYPE_BLOB);
            }
            buf = realloc (buf, len);
            binds[i].buffer_length = len;
            int ret = mysql_stmt_fetch_column (ps, &binds[i], i, 0);
            assert (ret != MYSQL_DATA_TRUNCATED && ret != MYSQL_NO_DATA);
            checked_call (ret, ps);
        }
        switch (binds[i].buffer_type) {
        case MYSQL_TYPE_NULL:
            rs << "null";
            break;
        case MYSQL_TYPE_LONGLONG:
            if (binds[i].is_unsigned)
                rs << "\"" << *(uint64_t *) buf << "\"";
            else
                rs << "\"" << *(int64_t *) buf << "\"";
            break;
        case MYSQL_TYPE_DOUBLE:
            rs << (double *) buf;
            break;
        case MYSQL_TYPE_STRING:
            print_json_str (rs, (char *) buf, len);
            break;
        case MYSQL_TYPE_BLOB:
            rs << "[";
            for (size_t j = 0; j < len; ++j) {
                if (j)
                    rs << ",";
                rs << ((unsigned char *) buf)[j];
            }
            rs << "]";
            break;
        case MYSQL_TYPE_TIMESTAMP:
            if (true) {
                MYSQL_TIME *t = (MYSQL_TIME *) buf;
                rs << "\"" << setw (4) << setfill ('0') << t->year
                   << "-" << setw (2) << t->month << "-" << t->day
                   << "T" << t->hour << ":" << t->minute << ":" << t->second
                   << "\"";
            }
            break;
        default:
            assert (0);
        }
    }
}

sql_res mysql_conn::execute (sql_stmt &&stmt)
{
    try {
        return real_exec (stmt);
    } catch (const coded_error &e) {
        if (e.code () == db_txn)
            close ();
        return sql_res (stmt, e.code (), e.what ());
    }
}

void mysql_conn::connect ()
{
    if (!(conn_ = mysql_init (0)))
        throw bad_alloc ();

    if (mysql_options (conn_, MYSQL_OPT_CONNECT_TIMEOUT, (char *) &timeout_)
        || mysql_options (conn_, MYSQL_OPT_READ_TIMEOUT, (char *) &timeout_)
        || mysql_options (conn_, MYSQL_OPT_WRITE_TIMEOUT, (char *) &timeout_))
        throw coded_error (db_txn, mysql_error (conn_));
    if (!mysql_real_connect (conn_, host_.c_str (), user_.c_str (),
                             password_.c_str (), db_.c_str (), port_, 0,
                             CLIENT_IGNORE_SIGPIPE))
        throw coded_error (db_txn, mysql_error (conn_));
}

static void clear_binds (vector<MYSQL_BIND> &binds)
{
    for (size_t i = 0; i < binds.size (); ++i) {
        if (binds[i].buffer)
            free (binds[i].buffer);
        if (binds[i].length)
            delete binds[i].length;
        if (binds[i].is_null)
            delete binds[i].is_null;
    }
    binds.clear ();
}

struct binds_clearer
{
    binds_clearer (vector<MYSQL_BIND> &binds) : binds_ (binds) {}
    ~binds_clearer () {clear_binds (binds_);}
private:
    vector<MYSQL_BIND> &binds_;
};

sql_res mysql_conn::real_exec (sql_stmt &&stmt)
{
    if (!conn_)
        connect ();

    if (stmt.builtin == sql_stmt::begin) {
        checked_call (mysql_autocommit (conn_, 0), conn_);
        return sql_res (stmt);
    } else if (stmt.builtin == sql_stmt::commit
               || stmt.builtin == sql_stmt::rollback) {
        typedef my_bool (*txn_ender) (MYSQL *);
        txn_ender f =
            stmt.builtin == sql_stmt::commit ? &mysql_commit : &mysql_rollback;
        checked_call (f (conn_), conn_);

        // set auto commit to default: true
        checked_call (mysql_autocommit (conn_, 1), conn_);
        return sql_res (stmt);
    }

    MYSQL_STMT *ps = 0;
    if (conn_ && stmts_.find (stmt.stmt->name) != stmts_.end ())
        ps = stmts_[stmt.stmt->name];
    else
        ps = stmts_[stmt.stmt->name] = stmt.stmt->prepare (conn_);
    size_t pc = mysql_stmt_param_count (ps);
    if ((pc && !stmt.params)
        || (pc && pc != (size_t) json_object_array_length (stmt.params)))
        throw coded_error (bad_arg, "wrong number of params");

    vector<MYSQL_BIND> binds (pc);
    binds_clearer bc (binds);

    for (size_t i = 0; i < pc; ++i)
        bind_param (&binds[i], json_object_array_get_idx (stmt.params, i));

    checked_call (mysql_stmt_bind_param (ps, &binds[0]), ps);
    checked_call (mysql_stmt_execute (ps), ps);

    clear_binds (binds);

    checked_call (mysql_stmt_store_result (ps), ps);

    if (stmt.stmt->insert_id) {
        uint64_t n = mysql_insert_id (conn_);
        return sql_res (move (stmt), string ("[[") + lexical_cast<string> (n)
                        + "]]");
    } else if (!stmt.stmt->is_query)
        return sql_res (stmt);

    // query, fetch the results
    binds.resize (stmt.stmt->results.size ());
    for (size_t i = 0; i < binds.size (); ++i)
        bind_res (&binds[i], stmt.stmt->results[i]);
    checked_call (mysql_stmt_bind_result (ps, &binds[0]), ps);
    ostringstream rs;
    rs << "[";
    size_t rows = mysql_stmt_affected_rows (ps);
    for (size_t r = 0; r < rows; ++r) {
        if (r)
            rs << ",";
        rs << "[";
        switch (mysql_stmt_fetch (ps)) {
        case 0: case MYSQL_DATA_TRUNCATED:
            gen_row_res (rs, ps, binds);
            break;
        case 1:
            checked_call (true, ps);
            break;
        case MYSQL_NO_DATA: default:
            assert (0);
        }
        rs << "]";
    }
    rs << "]";
    return sql_res (move (stmt), rs.str ());
}
