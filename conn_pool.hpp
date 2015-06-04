/// conn_pool.hpp -- connection pool decls

/// Author: Zhang Yichao <echaozh@gmail.com>
/// Created: 2011-08-02
///

#ifndef INCLUDED_CONN_POOL_HPP
#define INCLUDED_CONN_POOL_HPP

#include "sql_res.hpp"
#include "sql_stmt.hpp"

#include <zmq.hpp>

#include <boost/thread/mutex.hpp>

#include <pthread.h>

#include <deque>
#include <fstream>
#include <string>
#include <tr1/memory>
#include <tr1/unordered_map>
#include <tr1/unordered_set>

class mysql_conn;
class conn_pool
{
public:
    conn_pool (zmq::context_t &ctx, const std::string &listen,
               const std::string &host, unsigned short port,
               const std::string &user, const std::string &pass,
               const std::string &db, size_t db_timeout, size_t cap,
               size_t idle_timeout);
    ~conn_pool ();
    void start ();

public:
    template <typename FindDB>
    void init_stmts (const std::string &dir, const std::string &fn,
                     size_t timeout, FindDB find_db);

private:
    static void *serve (void *p);
    void real_serve ();
    void proc_req ();
    void proc_res (bool from_txn);

private:
    static void *proc (void *p);
    void real_proc (size_t n);
    sql_res proc_sqls (size_t n, sql_res &&res, mysql_conn &conn);
    sql_res proc_txn (size_t n, sql_res &&res, mysql_conn &conn,
                      size_t seq);
    size_t next_txn ();
    sql_stmt read_sql (size_t n, zmq::socket_t &sock);
    void write_res (zmq::socket_t &sock, sql_res &&res);

private:
    struct proc_arg
    {
        proc_arg (conn_pool &p, size_t m) : pool (p), n (m) {}
        conn_pool &pool;
        size_t n;
    };

private:
    boost::mutex lock_;
    pthread_t svrth_;
    std::deque<pthread_t> threads_;
    bool started_;
    size_t seq_;
    zmq::context_t &ctx_;
    std::string listen_;
    zmq::socket_t server_;
    zmq::socket_t sqls_;
    zmq::socket_t txns_;
    std::tr1::unordered_map<std::string, std::tr1::shared_ptr<mysql_stmt>
                            > stmts_;
    bool stmts_read_;
    // db related
    std::string host_;
    unsigned short port_;
    std::string user_;
    std::string password_;
    std::string db_;
    size_t db_timeout_;
    // other params
    size_t idle_timeout_;
};


template <typename FindDB>
std::string expand_dbs (const std::string &sql, FindDB find_db)
{
    using namespace std;

    if (sql.find_first_of ('$') == string::npos)
        return sql;

    ostringstream oss;
    size_t last = 0;
    while (true) {
        size_t dollar = sql.find_first_of ('$', last);
        if (dollar == string::npos)
            break;
        size_t dot = sql.find_first_of ('.', dollar + 1);
        if (dot == string::npos)
            throw runtime_error ("incorrect use of db name variable");
        string db_var = sql.substr (dollar + 1, dot - dollar - 1);
        string db = find_db (db_var);
        if (db.empty ())
            throw runtime_error ("unknown db name variable");
        oss << sql.substr (last, dollar - last);
        oss << db;
        last = dot;
    }

    oss << sql.substr (last);
    return oss.str ();
}

template <typename FindDB>
void add_stmt (std::tr1::unordered_map<std::string,
                                       std::tr1::shared_ptr<mysql_stmt>
                                       > &stmts, mysql_stmt *stmt,
               FindDB find_db)
{
    using namespace std;

    try {
        stmt->sql = expand_dbs (stmt->sql, find_db);
    } catch (const runtime_error &e) {
        cerr << stmt->file << ":" << stmt->lineno << ": " << stmt->name << ": "
             << e.what ();
        throw;
    }

    if (stmts[stmt->name]) {
        cerr << stmt->file << ":" << stmt->lineno << ": " << stmt->name
             << ": statement with the same time already defined, overwriting"
             << endl << flush;
    }
    stmts[stmt->name].reset (stmt);
}

template <typename FindDB>
void read_stmts (std::tr1::unordered_map<std::string,
                                         std::tr1::shared_ptr<mysql_stmt>
                                         > &stmts, const std::string &dir,
                 const std::string &fn,
                 std::tr1::unordered_set<std::string> &including,
                 FindDB find_db)
{
    using namespace std;

    string path = fn[0] == '/' ? fn : dir + '/' + fn;
    if (including.find (path) != including.end ()) {
        cerr << "circular inclusions of statements file found, "
             << "dead loop ahead, not proceeding" << endl << flush;
        throw runtime_error ("");
    }
    including.insert (path);

    ifstream f (path.c_str ());
    if (!f.is_open ()) {
        cerr << "failed to open statements file: " << path << ", cannot proceed"
             << endl << flush;
        throw runtime_error ("");
    }
    f.exceptions (ios::badbit);

    size_t lineno = 0;
    string line;

    string include;
    string name, sql;
    bool insert_id = false;

    while (!f.eof ()) {
        ++lineno;
        getline (f, line);

        // remove comment
        size_t pound = line.find_first_of ('#');
        if (pound != string::npos)
            line.erase (pound);

        size_t first = line.find_first_not_of (" \t");
        if (first == string::npos) {
            // empty line
            if (!sql.empty ()) {
                add_stmt (stmts, new mysql_stmt (name, sql, insert_id, fn,
                                                 lineno), find_db);
                name.clear ();
                sql.clear ();
                insert_id = false;
            }
            continue;
        }

        size_t last = line.find_last_not_of (" \t");
        size_t len = last == string::npos ? string::npos : last - first + 1;

        if (!name.empty ()) {
            // sqls can expand to multiple lines
            sql.append (" ");
            sql.append (line, first, len);
            continue;
        }

        const size_t len1 = sizeof ("include") - 1;
        if (!line.compare (first, len1, "include")
            && isblank (line[first + len1])) {
            // include another file
            size_t start = line.find_first_not_of (" \t", first + len1);
            read_stmts (stmts, dir, line.substr (start, last - start + 1),
                        including, find_db);
            continue;
        }

        // the sql name
        size_t end = line.find_first_of (": \t");
        if (end == string::npos)
            name = line;
        else {
            name = line.substr (first, end - first);
            size_t flags = line.find_first_not_of (" \t", end + 1);
            if (flags == string::npos)
                // no flags
                continue;
            const size_t len2 = sizeof ("insert-id") - 1;
            if ((!line.compare (flags, len2, "insert-id")
                 || !line.compare (flags, len2, "insert_id"))
                && (flags + len2 == line.size ()
                    || isblank (line[flags + len2])))
                insert_id = true;
        }
        if (name.empty ()) {
            cerr << fn << ":" << lineno << ": sql name should not be empty"
                 << endl << flush;
            throw runtime_error ("");
        }
    }

    if (!name.empty ()) {
        if (!sql.empty ()) {
            add_stmt (stmts, new mysql_stmt (name, sql, insert_id, fn, lineno),
                      find_db);
        } else {
            cerr << fn << ":" << lineno << ": " << name << ": no sql specified"
                 << endl << flush;
        }
    }

    including.erase (path);
}

template <typename FindDB>
void conn_pool::init_stmts (const std::string &dir, const std::string &fn,
                            size_t timeout, FindDB find_db)
{
    using namespace std;

    tr1::unordered_set<string> including;
    read_stmts (stmts_, dir, fn, including, find_db);

    MYSQL *conn = 0;
    tr1::unordered_map<string, tr1::shared_ptr<mysql_stmt> >::iterator it;
    for (it = stmts_.begin (); it != stmts_.end (); ++it) {
        it->second->init_results (conn, host_, port_, user_, password_, db_,
                                  timeout);
    }

    if (conn)
        mysql_close (conn);

    stmts_read_ = true;
}

#endif // INCLUDED_CONN_POOL_HPP
