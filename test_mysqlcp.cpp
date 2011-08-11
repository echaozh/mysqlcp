#include <json/json.h>
#include <cppzmq.hpp>

#include <boost/lexical_cast.hpp>

#include <cassert>
#include <iostream>
#include <sstream>
#include <string>

using namespace std;
using namespace boost;

static struct json_object *exec_sql (zmq::socket_t &sock,
                                     cppzmq::message_t &txn, size_t id,
                                     size_t &seq, const string &stmt,
                                     const string &params = "")
{
    ostringstream ss;
    ss << "{\"id\": " << id;
    if (seq)
        ss << ", \"txn\": " << seq;
    ss << ", \"sql\": \"" << stmt << "\"";
    if (!params.empty ())
        ss << ", \"params\": ";
    ss << params << "}";
    string s = ss.str ();
    cout << "req: " << s << endl << flush;
    cppzmq::message_t req (s);
    cppzmq::packet_t p;
    if (!txn.empty ())
        p.push_back (txn);
    p.push_back (req);

    sock << p;

    cppzmq::packet_t resp;
    sock >> resp;

    if (resp.size () == 2)
        txn = move (resp.front ());

    struct json_tokener *parser = json_tokener_new ();
    struct json_object *o =
        json_tokener_parse_ex (parser, (char *) resp.back ().data (),
                               resp.back ().size ());
    json_tokener_free (parser);

    seq = json_object_object_get (o, "txn")
        ? json_object_get_int (json_object_object_get (o, "txn")) : 0;
    int err = json_object_get_int (json_object_object_get (o, "code"));
    if (err) {
        cerr << "got error: "
             << json_object_get_string (json_object_object_get (o, "message"))
             << endl << flush;
        json_object_put (o);
        return 0;
    }

    struct json_object *res = json_object_object_get (o, "results");
    if (res) {
        json_object_get (res);
        json_object_object_del (o, "results");
        cout << "got results: " << json_object_to_json_string (res) << endl
             << flush;
    } else
        cout << "got no results" << endl << flush;

    json_object_put (o);
    return res;
}

int main ()
{
    zmq::context_t ctx (1);
    zmq::socket_t sock (ctx, ZMQ_REQ);
    sock.connect ("tcp://127.0.0.1:3406");

    size_t id = 1;
    cppzmq::message_t txn;
    size_t seq = 0;
    struct json_object *res;
    res = exec_sql (sock, txn, id++, seq, "begin");
    res = exec_sql (sock, txn, id++, seq, "test_select", "[1]");
    json_object_put (res);
    res = exec_sql (sock, txn, id++, seq, "test_insert", "[123, \"abc\"]");
    size_t iid = json_object_get_int (json_object_array_get_idx (
                                          json_object_array_get_idx (res, 0),
                                          0));
    json_object_put (res);
    string sel_param = string ("[") + lexical_cast<string> (iid) + "]";
    res = exec_sql (sock, txn, id++, seq, "test_select", sel_param);
    json_object_put (res);
    res = exec_sql (sock, txn, id++, seq, "test_delete", sel_param);
    assert (!res);
    res = exec_sql (sock, txn, id++, seq, "test_include", sel_param);
    json_object_put (res);
    res = exec_sql (sock, txn, id++, seq, "rollback");

    // second txn
    seq = 0;
    txn = cppzmq::message_t ();
    res = exec_sql (sock, txn, id++, seq, "begin");
    res = exec_sql (sock, txn, id++, seq, "test_insert", "[123, \"abc\"]");
    iid = json_object_get_int (json_object_array_get_idx (
                                   json_object_array_get_idx (res, 0), 0));
    json_object_put (res);
    sel_param = string ("[") + lexical_cast<string> (iid) + "]";
    res = exec_sql (sock, txn, id++, seq, "test_select", sel_param);
    json_object_put (res);
    res = exec_sql (sock, txn, id++, seq, "rollback");

    // no txn
    seq = 0;
    txn = cppzmq::message_t ();
    res = exec_sql (sock, txn, id++, seq, "test_select", sel_param);
    json_object_put (res);

    return 0;
}
