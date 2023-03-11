#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <nanomsg/nn.h>
#include <nanomsg/reqrep.h>

#include <protobuf-c/protobuf-c.h>
#include "aql.pb-c.h"
#include "common.h"

#define SERVER	"tcp://127.0.0.1:7777"
#define AQL_BUF_SIZE	1024

int client (const char *url, char *aql_query)
{
    int fd;
    int rc;

    fd = nn_socket(AF_SP, NN_REQ);
    if (fd < 0) {
        fprintf(stderr, "nn_socket: %s\n", nn_strerror(nn_errno()));
        return -1;
    }

    if (nn_connect (fd, url) < 0) {
        fprintf(stderr, "nn_socket: %s\n", nn_strerror(nn_errno()));
        nn_close(fd);
        return -1;
    }

	struct AQLDataPacked request_packed = pack_aql_request(aql_query);

	if (nn_send(fd, request_packed.payload, request_packed.size, 0) < 0) {
        fprintf(stderr, "nn_send: %s\n", nn_strerror(nn_errno()));
        nn_close(fd);
        return -1;
    }

	if (request_packed.payload) free(request_packed.payload);

	char *msg;

    rc = nn_recv(fd, &msg, NN_MSG, 0);
    if (rc < 0) {
        fprintf(stderr, "nn_recv: %s\n", nn_strerror(nn_errno()));
        nn_close(fd);
        return -1;
    }
    nn_close (fd);

	char *response = (char*) malloc(rc + 1);

    if (response == NULL) {
        fprintf(stderr, "malloc: %s\n", strerror(errno));
        return -1;
    }

    memcpy(response, msg, rc);
    response[rc] = '\0';

    nn_freemsg(msg);

    AQLServiceResponse *aql_response = aqlservice_response__unpack(NULL, rc, response);
    printf("rc: %d, payload: %s, status: %s, error: %s\n", rc, aql_response->payload, aql_response->status, aql_response->error);
    free(response);

    free(aql_response->error);
    free(aql_response);

    return 0;
}

int main (int argc, char **argv) {
	int rc;
	char aql_buf[AQL_BUF_SIZE];

	printf("AQL client.\n");
	printf("  Type query and press 'Enter'\n");
	printf("  Type 'quit' to exit.\n\n");

	for (;;) {
		memset(aql_buf, 0, AQL_BUF_SIZE);
		printf("aql> ");
		if (fgets(aql_buf, AQL_BUF_SIZE, stdin)) {
			aql_buf[strlen(aql_buf) - 1] = 0;
			if (strcmp("quit", aql_buf) == 0) {
				break;
			} else {
		    	rc = client(SERVER, aql_buf);
				if (rc) exit(EXIT_FAILURE);
			}
		}
	}

	return 0;
}
