/// main.cpp -- main program

/// Author: Zhang Yichao <echaozh@gmail.com>
/// Created: 2011-08-02
///

#include "conn_pool.hpp"

#include <vconf/vconf.h>

#include <mysql/mysql.h>
#include <zmq.hpp>

#include <boost/lexical_cast.hpp>

#include <libgen.h>

#include <cassert>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

using namespace std;
using namespace boost;

static string s_host, s_port;
static string s_stmts_file;
static uint32_t s_db_timeout, s_pool_cap, s_idle_timeout;

static string working_dir (int argc, char **argv)
{
    if (argc == 1) {
        char *program = strdup (argv[0]);
        string dir = dirname (program);
        free (program);
        return  dir.empty () ? "/" : dir + "/../";
    } else
        return string (argv[1]) + "/";
}

static struct vconf *read_config (int argc, char **argv)
{
    string cf = working_dir (argc, argv) + "etc/mysqlcp.conf";
    ifstream f (cf.c_str ());
    f.seekg (0, ios::end);
    vector<char> s ((size_t) f.tellg () + 1);
    f.seekg (0, ios::beg);
    f.read (&s[0], s.size ());
    s[s.size () - 1] = 0;
    return vconf_new (&s[0]);
}

static void parse_config (struct vconf *conf)
{
    s_host = "0.0.0.0";
    s_port = "3406";
    struct vconf_url *listen = vconf_get_url (conf, "listen_address");
    if (listen) {
        s_host = listen->host;
        s_port = lexical_cast<string> (listen->port);
        vconf_free_url (listen);
    }
    clog << "listening at: " << s_host << ":" << s_port << endl << flush;

    const char *s = vconf_get_string (conf, "sql_file");
    s_stmts_file = s && s[0] ? s : "sqls";
    clog << "reading statements from file: " << s_stmts_file << endl << flush;

    if (vconf_get_uint (conf, "mysql_conn_timeout", &s_db_timeout))
        s_db_timeout = 180;
    clog << "setting mysql connection timeout to " << s_db_timeout << endl
         << flush;
    if (vconf_get_uint (conf, "conn_pool_capacity", &s_pool_cap))
        s_pool_cap = 100;
    clog << "setting connection pool capacity to " << s_pool_cap << endl
         << flush;
    if (vconf_get_uint (conf, "txn_idle_timeout", &s_idle_timeout))
        s_idle_timeout = 600;
    else if (s_idle_timeout > 1800) {
        cerr << "transaction idle timeout too long: " << s_idle_timeout << endl
             << flush;
        s_idle_timeout = 1800;
    }
    clog << "setting transaction idle timeout to " << s_idle_timeout << endl
         << flush;
}

struct find_from_conf
{
    find_from_conf (const struct vconf *cf) : conf_ (cf) {}
    string operator () (const string &var)
        {
            const char *v = vconf_get_string (conf_, var.c_str ());
            return v ?: "";
        }
private:
    const struct vconf *conf_;
};

int main (int argc, char **argv)
{
    struct vconf *conf = read_config (argc, argv);
    struct vconf_url *db = vconf_get_url (conf, "backend_db");
    if (!db) {
        cerr << "backend db not configured, cannot proceed" << endl << flush;
        exit (1);
    } else if (!db->host || !*db->host || !db->user || !*db->user) {
        cerr << "db not properly configured, cannot proceed" << endl << flush;
        exit (1);
    } else if (!db->port)
        db->port = 3306;
    clog << "connecting to backend db at: " << db->user << "@" << db->host
         << ":" << db->port;
    if (db->path && db->path[0] && db->path[1])
        clog << "/" << &db->path[1] << endl << flush;

    parse_config (conf);

    if (mysql_library_init (0, 0, 0)) {
        cerr << "failed to init mysql library, cannot proceed" << endl << flush;
        exit (1);
    }

    zmq::context_t ctx (1);

    string listen = string ("tcp://") + s_host + ":" + s_port;
    conn_pool pool (ctx, listen, db->host, db->port, db->user,
                    db->password ?: "", db->path ? &db->path[1] : "",
                    s_db_timeout, s_pool_cap, s_idle_timeout);

    pool.init_stmts (working_dir (argc, argv) + "etc/", s_stmts_file,
                     s_db_timeout, find_from_conf (conf));
    vconf_free (conf);

    pool.start ();

    vconf_free_url (db);

    while (true)
        ;

    mysql_library_end ();
    return 0;
}
