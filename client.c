#include "client.h"

// {"TP":"","T":""}
char *get_header(statement *stmt) {
    if (!stmt) return "";
    char *serialized = (char*) malloc(strlen("{'TP':' ','T':''}") + strlen(stmt->table) + 1);
    memset(serialized, 0, strlen("{'TP':' ','T':''}") + strlen(stmt->table) + 1);
    sprintf(serialized, "{\"TP\":\"%d\",\"T\":\"%s\"}", stmt->stmt_type, stmt->table);

    return serialized;
}

char *serialize_literal(literal *lit) {
    if (!lit) return "";
    char *serialized = (char*) malloc(strlen("{'T':' ','V':''}") + strlen(lit->value) + 1);
    memset(serialized, 0, strlen("{'T':' ','V':''}") + strlen(lit->value) + 1);
    sprintf(serialized, "{\"T\":\"%d\",\"V\":\"%s\"}", lit->type, lit->value);

    return serialized;
}

char *serialize_field_list(field *f) {

}

char *serialize_cell_list(cell *c) {

}

char *serialize_var_list(table_var_link *vars) {

}

char *serialize_ref_list(reference *refs) {

}

char *serialize_predicate(predicate *pred) {

}

char *serialize_create_stmt(create_stmt *create_stmt) {

}

char *serialize_select_stmt(select_stmt *select_stmt) {

}

char *serialize_insert_stmt(insert_stmt *insert_stmt) {

}

char *serialize_update_stmt(update_stmt *update_stmt) {

}

char *serialize_delete_stmt(delete_stmt *delete_stmt) {

}

char *serialize_stmt(statement *stmt) {
    char *serialized = (char*) malloc(2);

    print_statement(stmt);

    serialized[0] = 'A';
    serialized[1] = 0;

    return serialized;
}

void send_and_receive(char *address, statement *stmt) {
    char *stmt_header = get_header(stmt);
    char *str_stmt = serialize_stmt(stmt);

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