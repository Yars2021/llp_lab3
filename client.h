#ifndef __AQL_CLIENT_H__
#define __AQL_CLIENT_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <nanomsg/nn.h>
#include <nanomsg/reqrep.h>

#include <protobuf-c/protobuf-c.h>
#include "data.h"
#include "aql.pb-c.h"
#include "common.h"

#define AQL_BUF_SIZE 1024
#define DEFAULT_SERVER "tcp://127.0.0.1:7777"

char *get_header(statement *stmt);
char *serialize_stmt(statement *stmt);
void send_and_receive(char *address, statement *stmt);

#endif
