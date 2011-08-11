/// conn_pool.cpp -- conn pool impl

/// Copyright (c) 2011 Vobile. All rights reserved.
/// Author: Zhang Yichao <zhang_yichao@vobile.cn>
/// Created: 2011-08-02
///

#include "conn_pool.hpp"
#include "mysql_conn.hpp"
#include "sql_res.hpp"
#include "sql_stmt.hpp"

#include <cppzmq.hpp>
#include <vconf/vconf.h>

#include <boost/thread/locks.hpp>

#include <pthread.h>

#include <cassert>
#include <climits>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <stdexcept>

using namespace std;
using namespace boost;

conn_pool::conn_pool (zmq::context_t &ctx, const string &listen,
                      const string &host, unsigned short port,
                      const string &user, const string &pass, const string &db,
                      size_t db_timeout, size_t cap, size_t idle_timeout)
    : threads_ (cap), started_ (false), seq_ (0), ctx_ (ctx),
      listen_ (listen), server_ (ctx_, ZMQ_XREP), sqls_ (ctx_, ZMQ_XREQ),
      txns_ (ctx_, ZMQ_XREP), stmts_read_ (false), host_ (host), port_ (port),
      user_ (user), password_ (pass), db_ (db), db_timeout_ (db_timeout),
      idle_timeout_ (idle_timeout)
{
    if (listen.empty ())
        throw invalid_argument ("bad listening address");
    if (host.empty () || !port || user.empty ())
        throw invalid_argument ("bad db configuration");
}

conn_pool::~conn_pool ()
{
    if (!started_)
        return;

    for (size_t i = 0; i < threads_.size (); ++i)
        pthread_join (threads_[i], 0);
}

sql_stmt conn_pool::read_sql (size_t n, zmq::socket_t &sock)
{
    cppzmq::packet_t req;
    sock >> req;
    cppzmq::packet_t addr = req.unseal ();
    assert (req.size () == 1);

    return sql_stmt (addr, req.front (), stmts_);
}

void conn_pool::write_res (zmq::socket_t &sock, sql_res &&res)
{
    if (res.empty)
        return;

    ostringstream ss;
    ss << "{";
    if (res.id)
        ss << "\"id\": " << res.id << ", ";
    ss << "\"code\": " << res.err << ", \"message\": \"" << res.msg << "\"";
    if (res.txn_seq)
        ss << ", \"txn\": " << res.txn_seq;
    if (!res.err && !res.res.empty ())
        ss << ", \"results\": " << res.res;
    ss << "}";

    cppzmq::message_t msg (ss.str ());
    cppzmq::packet_t p (std::move (res.addr));
    p.push_back (msg);

    sock << p;
}

sql_res conn_pool::proc_sqls (size_t n, sql_res &&res, mysql_conn &conn)
{
    zmq::socket_t sqls (ctx_, ZMQ_DEALER);
    sqls.connect ("inproc://sql-dealer");

    write_res (sqls, res);

    while (true) {
        sql_stmt sql = read_sql (n, sqls);
        if (sql.err) {
            write_res (sqls, sql_res (sql));
            continue;
        } else if (sql.txn_seq) {
            // we're not doing txn here
            write_res (sqls, sql_res (move (sql), bad_txn));
            continue;
        }
        sql_res res = conn.execute (sql);
        if (sql.begins_txn ())
            return res;
        else
            write_res (sqls, res);
    }
}

sql_res conn_pool::proc_txn (size_t n, sql_res &&res, mysql_conn &conn,
                             size_t seq)
{
    zmq::socket_t txn (ctx_, ZMQ_DEALER);
    txn.connect ("inproc://txn-router");

    assert (!res.empty);
    cppzmq::packet_t addr (res.addr);
    write_res (txn, res);

    while (true) {
        zmq_pollitem_t polls[1] = {{txn, -1, ZMQ_POLLIN, 0}};
        int ret = zmq::poll (polls, 1, idle_timeout_ * 1000000);
        if (ret == 0) {
            // txn timed out, exit the txn
            conn.rollback ();
            // conn.close ();
            return sql_res (addr, seq, txn_timeout);
        }

        assert (ret == 1);
        assert (polls[0].revents & ZMQ_POLLIN);
        sql_stmt sql = read_sql (n, txn);
        if (sql.err)
            write_res (txn, sql_res (move (sql)));
        else if (sql.begins_txn ()) {
            return sql_res (move (sql), bad_txn,
                            "nested transactions not allowed");
        } else if (sql.txn_seq != seq)
            write_res (txn, sql_res (move (sql), bad_txn));
        else if (addr.back () != sql.addr.back ())
            write_res (txn, sql_res (move (sql), bad_caller));
        else {
            sql_res res = conn.execute (sql);
            if (sql.ends_txn () || res.err == db_txn)
                return res;
            else
                write_res (txn, res);
        }
    }
}

void conn_pool::real_proc (size_t n)
{
    mysql_conn conn (host_, port_, user_, password_, db_, db_timeout_);

    sql_res res;
    while (true) {
        res = proc_sqls (n, move (res), conn);
        size_t seq = next_txn ();
        res.txn_seq = seq;
        res = proc_txn (n, move (res), conn, seq);
    }
}

void *conn_pool::proc (void *p)
{
    assert (p);
    proc_arg *arg = (proc_arg *) p;
    conn_pool &pool = arg->pool;
    size_t n = arg->n;
    delete arg;
    pool.real_proc (n);
    return 0;
}

void conn_pool::start ()
{
    if (!stmts_read_)
        throw logic_error ("init statements first, and then start pool");

    if (started_)
        return;

    // create the zmq sockets
    server_.bind (listen_.c_str ());
    sqls_.bind ("inproc://sql-dealer");
    txns_.bind ("inproc://txn-router");

    // start the exec threads
    pthread_attr_t attr;
    if (pthread_attr_init (&attr))
        throw bad_alloc ();
    if (pthread_attr_setstacksize (&attr, 64 << 10))
        throw invalid_argument ("bad stack size");
    for (size_t i = 0; i < threads_.size (); ++i) {
        if (pthread_create (&threads_[i], &attr, &conn_pool::proc,
                            new proc_arg (*this, i)))
            throw runtime_error ("failed to create more threads");
    }

    // start the server thread
    if (pthread_create (&svrth_, &attr, &conn_pool::serve, this))
        throw runtime_error ("failed to create more threads");

    started_ = true;
}

size_t conn_pool::next_txn ()
{
    // NOTE: we enforce a txn seq no on every txn, so when a txn times out, it
    //       won't leak to another executor happening to have the same identity
    unique_lock<mutex> lk (lock_);
    if (seq_++ == INT_MAX)
        seq_ = 1;
    return seq_;
}

void *conn_pool::serve (void *p)
{
    assert (p);
    ((conn_pool *) p)->real_serve ();
    return 0;
}

void conn_pool::proc_res (bool from_txn)
{
    cppzmq::packet_t res;
    (from_txn ? txns_ : sqls_) >> res;
    cppzmq::packet_t p = res.unseal ();
    assert (res.size () == 1);

    if (from_txn) {
        cppzmq::message_t txn = move (p.front ());
        txn.label (false);
        p.pop_front ();
        p.push_back (txn);
    }
    p.push_back (move (res.front ()));

    server_ << p;
}

static void send_bad_proto (zmq::socket_t &server, cppzmq::packet_t &&addr)
{
    cppzmq::message_t bad_proto ("{\"code\": 1, "
                                 "\"message\": \"bad protocol\"}");
    cppzmq::packet_t p (addr);
    p.push_back (bad_proto);
    server << addr;
}

void conn_pool::proc_req ()
{
    cppzmq::packet_t req;
    server_ >> req;
    cppzmq::packet_t p = req.unseal ();

    if (req.size () != 1 && req.size () != 2) {
        send_bad_proto (server_, p);
        return;
    }

    bool to_txn = req.size () == 2;
    if (to_txn) {
        cppzmq::message_t txn = move (req.front ());
        req.pop_front ();
        txn.label (true);
        p.push_front (move (txn));
    }
    p.push_back (req.front ());

    (to_txn ? txns_ : sqls_) << p;
}

void conn_pool::real_serve ()
{
    zmq_pollitem_t polls[3] = {
        {server_, -1, ZMQ_POLLIN, 0},
        {sqls_, -1, ZMQ_POLLIN, 0},
        {txns_, -1, ZMQ_POLLIN, 0}
    };
    while (true) {
        zmq::poll (polls, 3);

        // NOTE: receiving from ZMQ_REP & ZMQ_DEALER sockets are very different
        //       REP sockets automatically appends a blank delimiter to the
        //       front of the message, and the DEALER ones don't
        //       DEALER callers must append the blank message manually
        if (polls[0].revents & ZMQ_POLLIN)
            proc_req ();
        if (polls[1].revents & ZMQ_POLLIN)
            proc_res (false);
        if (polls[2].revents & ZMQ_POLLIN)
            proc_res (true);
    }

}
