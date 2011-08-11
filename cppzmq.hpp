/// cppzmq.hpp -- cpp impl of libczmq frames and messages

/// Copyright (c) 2011 Vobile. All rights reserved.
/// Author: Zhang Yichao <zhang_yichao@vobile.cn>
/// Created: 2011-08-08
///

#ifndef INCLUDED_CPPZMQ_HPP
#define INCLUDED_CPPZMQ_HPP

#include <zmq.hpp>

#include <cstring>
#include <deque>
#include <stdexcept>
#include <string>

namespace cppzmq
{
    class message_t
    {
    public:
        message_t () : label_ (false) {}
        message_t (const message_t &rhs) : label_ (rhs.label_)
            {msg_.copy (&rhs.msg_);}
        message_t (message_t &&rhs) : label_ (rhs.label_)
            {msg_.move (&rhs.msg_);}
        message_t (size_t size) : msg_ (size), label_ (false) {}
        message_t (void *data, size_t size, zmq::free_fn *ffn, void *hint = 0)
            : msg_ (data, size, ffn, hint), label_ (false) {}
        message_t (const char *s, size_t len = (size_t) -1)
            : msg_ (len == (size_t) -1 ? strlen (s) : len), label_ (false)
            {memcpy (msg_.data (), s, msg_.size ());}
        message_t (const std::string &s) : msg_ (s.size ()), label_ (false)
            {memcpy (msg_.data (), s.data (), msg_.size ());}
        ~message_t () {}
        message_t &operator = (const message_t &rhs)
            {
                msg_.copy (&rhs.msg_);
                label_ = rhs.label_;
                return *this;
            }
        message_t &operator = (message_t &&rhs)
            {
                msg_.move (&rhs.msg_);
                label_ = rhs.label_;
                return *this;
            }
        bool operator == (const message_t &rhs)
            {
                if (size () != rhs.size ())
                    return false;
                return memcmp (data (), rhs.data (), size ());
            }
        bool operator != (const message_t &rhs)
            {
                return !(*this == rhs);
            }
        bool empty () const {return !size ();}
        const void *data () const
            {return const_cast<message_t *> (this)->data ();}
        void *data () {return msg_.data ();}
        size_t size () const {return msg_.size ();}
        bool label () const {return label_;}
        void label (bool l) {label_ = l;}

    public:
        void send (zmq::socket_t &sock, bool more = false) const
            {
                sock.send (msg_, (more ? ZMQ_SNDMORE : 0)
                           | (label_ ? ZMQ_SNDLABEL : 0));
            }
        bool recv (zmq::socket_t &sock)
            {
                sock.recv (&msg_);
                int flag;
                size_t flagsz = sizeof (flag);
                sock.getsockopt (ZMQ_RCVLABEL, &flag, &flagsz);
                label_ = flag;
                if (flag)
                    return true;
                sock.getsockopt (ZMQ_RCVMORE, &flag, &flagsz);
                return flag;
            }

    private:
        mutable zmq::message_t msg_;
        bool label_;
    };

    class packet_t
    {
    public:
        packet_t () {}
        packet_t (packet_t &&rhs) : msgs_ (std::move (rhs.msgs_)) {}
        ~packet_t () {}
        bool empty () const {return msgs_.empty ();}
        size_t size () const {return msgs_.size ();}
        const message_t &front () const {return msgs_.front ();}
        message_t &front () {return msgs_.front ();}
        const message_t &back () const {return msgs_.back ();}
        message_t &back () {return msgs_.back ();}
        void push_front (const message_t &m)
            {
                check_label_front (m);
                msgs_.push_front (m);
            }
        void push_front (const message_t &&m)
            {
                check_label_front (m);
                msgs_.push_front (m);
            }
        void pop_front () {msgs_.pop_front ();}
        void push_back (const message_t &m)
            {
                check_label_back (m);
                msgs_.push_back (m);
            }
        void push_back (const message_t &&m)
            {
                check_label_back (m);
                msgs_.push_back (m);
            }
        void pop_back () {msgs_.pop_back ();}

    public:
        packet_t unseal ()
            {
                packet_t envelop;
                while (!msgs_.empty () && msgs_.front ().label ()) {
                    envelop.msgs_.push_back (std::move (msgs_.front ()));
                    msgs_.pop_front ();
                }
                return envelop;
            }
        void seal (const packet_t &p)
            {
                msgs_.insert (msgs_.begin (), p.msgs_.begin (), p.msgs_.end ());
                for (size_t i = 0; i < p.msgs_.size (); ++i)
                    msgs_[i].label (true);
            }

    public:
        void send (zmq::socket_t &sock) const
            {
                for (std::deque<message_t>::const_iterator it = msgs_.begin ();
                     it != msgs_.end (); ++it)
                    it->send (sock, it + 1 != msgs_.end ());
            }
        void recv (zmq::socket_t &sock)
            {
                msgs_.clear ();
                do {
                    msgs_.push_back (message_t ());
                } while (msgs_.back ().recv (sock));
            }

    private:
        void check_label_front (const message_t &m)
            {
                if (!m.label () && !msgs_.empty () && msgs_.front ().label ())
                    throw std::invalid_argument ("message has to be labeled");
            }
        void check_label_back (const message_t &m)
            {
                if (m.label () && !msgs_.empty () && !msgs_.back ().label ())
                    throw std::invalid_argument ("message cannot be labeled");
            }

    private:
        std::deque<message_t> msgs_;
    };

    static inline zmq::socket_t &operator << (zmq::socket_t &sock,
                                              const packet_t &p)
    {
        p.send (sock);
        return sock;
    }

    static inline zmq::socket_t &operator >> (zmq::socket_t &sock, packet_t &p)
    {
        p.recv (sock);
        return sock;
    }
}

#endif // INCLUDED_CPPZMQ_HPP
