/// sql_stmt.cpp -- sql statement impl

/// Author: Zhang Yichao <echaozh@gmail.com>
/// Created: 2011-08-03
///

#include "exception.hpp"
#include "sql_stmt.hpp"

using namespace std;

struct json_putter
{
    json_putter (struct json_object *p) : p_ (p) {}
    ~json_putter () {json_object_put (p_);}
private:
    struct json_object *p_;
};

sql_stmt::sql_stmt (cppzmq::packet_t &&a, const cppzmq::message_t &sql,
                    const tr1::unordered_map<string, tr1::shared_ptr<mysql_stmt>
                                             > &stmts)
    : addr (a), id (0), err (success), txn_seq (0), builtin (none), params (0)
{
    try {
        struct json_tokener *parser = json_tokener_new ();
        if (!parser)
            throw bad_alloc ();

        struct json_object *parsed =
            json_tokener_parse_ex (parser, (char *) sql.data (), sql.size ());
        json_tokener_free (parser);
        if (!parsed)
            throw coded_error (bad_req, "malformed json");

        json_putter jf (parsed);
        struct json_object *p = json_object_object_get (parsed, "id");
        if (!p)
            throw coded_error (bad_req, "no id specified");
        id = json_object_get_int (p);
        if (!id)
            throw coded_error (bad_req, "no id specified");

        p = json_object_object_get (parsed, "sql");
        if (!p)
            throw coded_error (bad_req, "no statement specified");
        string name = json_object_get_string (p);
        if (name == "begin")
            builtin = begin;
        else if (name == "commit")
            builtin = commit;
        else if (name == "rollback")
            builtin = rollback;
        else {
            if (stmts.find (name) == stmts.end ())
                throw coded_error (bad_req, "unknown statement");
            stmt = stmts.find (name)->second;
        }

        p = json_object_object_get (parsed, "txn");
        if (p)
            txn_seq = json_object_get_int (p);

        params = json_object_object_get (parsed, "params");
        if (params) {
            if (!json_object_is_type (params, json_type_array))
                throw coded_error (bad_arg, "params must be an array");
            json_object_get (params);
            json_object_object_del (parsed, "params");
        }
    } catch (const coded_error &e) {
        err = e.code ();
        msg = e.what ();
    }
}
