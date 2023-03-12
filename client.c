#include "client.h"

// {"TP":"","T":""}
char *get_header(statement *stmt) {
    if (!stmt) return "";
    char *serialized = (char*) malloc(strlen("{'TP':' ','T':''}") + strlen(stmt->table) + 1);
    memset(serialized, 0, strlen("{'TP':' ','T':''}") + strlen(stmt->table) + 1);
    sprintf(serialized, "{\"TP\":\"%d\",\"T\":\"%s\"}", stmt->stmt_type, stmt->table);

    return serialized;
}

void send_and_receive(char *address, statement *stmt) {
    char *stmt_header = get_header(stmt);
    char *str_stmt = serialize_statement(stmt);

    int fd;
    int rc;

    fd = nn_socket(AF_SP, NN_REQ);
    if (fd < 0) {
        fprintf(stderr, "nn_socket: %s\n", nn_strerror(nn_errno()));
        return;
    }

    if (nn_connect (fd, address) < 0) {
        fprintf(stderr, "nn_socket: %s\n", nn_strerror(nn_errno()));
        nn_close(fd);
        return;
    }

    struct AQLDataPacked request_packed = pack_aql_request(stmt_header, str_stmt);

    if (nn_send(fd, request_packed.payload, request_packed.size, 0) < 0) {
        fprintf(stderr, "nn_send: %s\n", nn_strerror(nn_errno()));
        nn_close(fd);
        return;
    }

    if (request_packed.payload) free(request_packed.payload);
    free(stmt_header);
    free(str_stmt);

    char *msg;

    rc = nn_recv(fd, &msg, NN_MSG, 0);
    if (rc < 0) {
        fprintf(stderr, "nn_recv: %s\n", nn_strerror(nn_errno()));
        nn_close(fd);
        return;
    }
    nn_close (fd);

    char *response = (char*) malloc(rc + 1);

    if (response == NULL) {
        fprintf(stderr, "malloc: %s\n", strerror(errno));
        return;
    }

    memcpy(response, msg, rc);
    response[rc] = '\0';

    nn_freemsg(msg);

    AQLServiceResponse *aql_response = aqlservice_response__unpack(NULL, rc, response);
    printf("rc: %d, payload: %s, status: %s, error: %s\n", rc, aql_response->payload, aql_response->status, aql_response->error);
    free(response);

    aqlservice_response__free_unpacked(aql_response, NULL);
}