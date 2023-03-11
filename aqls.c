#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <nanomsg/nn.h>
#include <nanomsg/reqrep.h>

#include <protobuf-c/protobuf-c.h>
#include "aql.pb-c.h"
#include "common.h"

#define SERVER	"tcp://127.0.0.1:7777"

#define MSG_IN	"IN"
#define MSG_OUT	"OUT"

void aql_log(const char *direction, const char *message) {
	time_t now;
	time(&now);
    char buffer[26];
    struct tm* tm_info;

    now = time(NULL);
    tm_info = localtime(&now);
    strftime(buffer, 26, "%Y-%m-%d %H:%M:%S", tm_info);

	printf("%s [%3s]: %s\n", buffer, direction, message);
}

int server(const char *url)
{
    int fd; 

    fd = nn_socket(AF_SP, NN_REP);
    if (fd < 0) {
        fprintf(stderr, "nn_socket: %s\n", nn_strerror(nn_errno()));
        return -1;
    }

    if (nn_bind(fd, url) < 0) {
        fprintf(stderr, "nn_bind: %s\n", nn_strerror(nn_errno()));
        nn_close(fd);
        return -1;
    }

    struct AQLDataPacked response_packed;

    for (;;) {
		char aql_query[4096];
		int rc;

        rc = nn_recv(fd, aql_query, sizeof(aql_query), 0);
        if (rc < 0) {
            fprintf (stderr, "nn_recv: %s\n", nn_strerror(nn_errno()));
            break;
        }

		AQLServiceRequest *aql_request = aqlservice_request__unpack(NULL, rc, aql_query);

		printf("rc: %d, payload: %s(%ld)\n", rc, aql_request->payload, strlen(aql_request->payload));

		if (aql_request == NULL) aql_log(MSG_OUT, "NULL");
        else {
            if (rc < strlen(aql_request->payload)) {
                aql_request->payload[rc] = '\0';
            } else {
                aql_request->payload[strlen(aql_request->payload)] = '\0';
            }

            aql_log(MSG_IN, aql_request->payload);

            /* parse query */

            /* execute query  */

            /* serialize response */

            AQLServiceError *serviceError = (AQLServiceError*) malloc(sizeof(AQLServiceError));
            serviceError->msg = "err.msg";
            serviceError->details = "err.details";
            response_packed = pack_aql_response("RESPONSE TEXT", AQLSERVICE_STATUS__OK, serviceError);

            printf("HERE");

            if (nn_send(fd, response_packed.payload, response_packed.size, 0) < 0) {
                fprintf(stderr, "nn_send: %s (ignored)\n", nn_strerror(nn_errno()));
                nn_close(fd);
            }

            aql_log(MSG_OUT, "Message sent");

            if (response_packed.payload) free(response_packed.payload);
            free(serviceError);
        }
    }

    nn_close (fd);
    return 0;
}

int main(int argc, char** argv) {
	int rc = server(SERVER);
	exit(rc == 0 ? EXIT_SUCCESS : EXIT_FAILURE);
}
