/*
    Modified for junban in 2026 by junban contributors.
    Task Spooler - a task queue system for the unix user
    Copyright (C) 2007-2009  Lluís Batlle i Rossell

    Please find the license in the provided COPYING file.
*/
#include <sys/types.h>
#include <sys/socket.h>
#include <stdio.h>
#include <sys/time.h>
#include <stdlib.h>
#include <errno.h>
#include "main.h"

void send_bytes(const int fd, const char *data, int bytes)
{
    int res;
    int offset = 0;

    while(bytes > 0)
    {
        res = send(fd, data + offset, bytes, 0);
        if (res < 0 && errno == EINTR) continue;
        if(res <= 0)
        {
            /* A timed-out server write drops the connection once. Later
             * pieces of this reply must fail immediately, not each wait
             * for another timeout. The event loop performs job cleanup. */
            if (process_type == SERVER) shutdown(fd, SHUT_RDWR);
            break;
        }
        if(res == bytes)
            break;
        offset += res;
        bytes -= res;
    }
}

int recv_bytes(const int fd, char *data, int bytes)
{
    int res;
    int offset = 0;

    int total = bytes;
    while(bytes > 0)
    {
        res = recv(fd, data + offset, bytes, 0);
        if (res < 0 && errno == EINTR) continue;
        if(res <= 0) {
            if (process_type == SERVER) shutdown(fd, SHUT_RDWR);
            return offset == 0 ? res : -1;
        }
        offset += res;
        bytes -= res;
    }

    return total;
}

void send_msg(const int fd, const struct msg *m)
{
    send_bytes(fd, (const char *)m, sizeof(*m));
}

int recv_msg(const int fd, struct msg *m)
{
    return recv_bytes(fd, (char *)m, sizeof(*m));
}
