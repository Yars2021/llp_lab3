#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <regex.h>

#include <nanomsg/nn.h>
#include <nanomsg/reqrep.h>

#include <protobuf-c/protobuf-c.h>
#include "aql.pb-c.h"
#include "common.h"
#include "data.h"
#include "aql.tab.h"

#include "lib_database/db_internals.h"
#include "lib_database/db_file_manager.h"

#define MSG_IN	"IN"
#define MSG_OUT	"OUT"

typedef struct {
    int type;
    char *table;
} RequestHeader;

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

RequestHeader *parseRequestHeader(char *common_header) {
    if (!common_header) return NULL;

    regex_t regexp_rec;
    regmatch_t groups[3];
    int comp_ec, exec_ec;

    comp_ec = regcomp(&regexp_rec, "^\\{\"TP\":\"(.)\",\"T\":\"(.*)\"\\}$", REG_EXTENDED);

    if (comp_ec != REG_NOERROR) return NULL;

    exec_ec = regexec(&regexp_rec, common_header, 3, groups, 0);

    if (exec_ec == REG_NOMATCH) return NULL;

    RequestHeader *requestHeader = (RequestHeader*) malloc(sizeof(RequestHeader));
    requestHeader->type = common_header[groups[1].rm_so] - '0';
    requestHeader->table = substrToNewInstance(common_header, groups[2].rm_so, groups[2].rm_eo);

    regfree(&regexp_rec);

    return requestHeader;
}

Table *parseCreateRequest(char *table_name, char *payload) {
    if (!payload) return NULL;

    regex_t regexp_rec;
    regmatch_t groups[3];
    int comp_ec, exec_ec;

    comp_ec = regcomp(&regexp_rec, "^\\{\"fn\":\"(.*)\",\"f\":\\[(.*)\\]\\}$", REG_EXTENDED);

    if (comp_ec != REG_NOERROR) return NULL;

    exec_ec = regexec(&regexp_rec, payload, 3, groups, 0);

    if (exec_ec == REG_NOMATCH) return NULL;

    char *s_field_number = substrToNewInstance(payload, groups[1].rm_so, groups[1].rm_eo);
    char *s_fields = substrToNewInstance(payload, groups[2].rm_so, groups[2].rm_eo);
    size_t num_of_fields = string_to_size_t(s_field_number);

    regfree(&regexp_rec);


    char *field_reg = "\\{\"n\":\"(.*)\",\"t\":\"(.)\"\\},";
    size_t offset = 1;

    char *fields_format = (char*) malloc(strlen(field_reg) * num_of_fields + 2);
    fields_format[0] = '^';
    for (size_t i = 0; i < num_of_fields; i++, offset += strlen(field_reg)) memcpy(fields_format + offset, field_reg, strlen(field_reg) + 1);
    fields_format[offset - 1] = '$';
    fields_format[offset] = '\0';

    regex_t regexp_fields_rec;

    regmatch_t field_groups[2 * num_of_fields + 1];
    int comp_fields_ec, exec_fields_ec;

    comp_fields_ec = regcomp(&regexp_fields_rec, fields_format, REG_EXTENDED);

    if (comp_fields_ec != REG_NOERROR) return NULL;

    exec_fields_ec = regexec(&regexp_fields_rec, s_fields, 2 * num_of_fields + 1, field_groups, 0);

    if (exec_fields_ec == REG_NOMATCH) return NULL;

    free(fields_format);
    regfree(&regexp_fields_rec);

    Field **fields = (Field**) malloc(sizeof(Field*) * num_of_fields);

    for (size_t i = 1; i <= num_of_fields; i++) {
        switch (s_fields[field_groups[2 * i].rm_so]) {
            case '1':
                fields[i - 1] = createField(substrToNewInstance(s_fields, field_groups[2 * i - 1].rm_so, field_groups[2 * i - 1].rm_eo), INTEGER_F);
                break;
            case '2':
                fields[i - 1] = createField(substrToNewInstance(s_fields, field_groups[2 * i - 1].rm_so, field_groups[2 * i - 1].rm_eo), FLOAT_F);
                break;
            case '3':
                fields[i - 1] = createField(substrToNewInstance(s_fields, field_groups[2 * i - 1].rm_so, field_groups[2 * i - 1].rm_eo), BOOLEAN_F);
                break;
            default:
                fields[i - 1] = createField(substrToNewInstance(s_fields, field_groups[2 * i - 1].rm_so, field_groups[2 * i - 1].rm_eo), STRING_F);
                break;
        }
    }

    free(s_field_number);
    free(s_fields);

    return createTable(createTableSchema(fields, num_of_fields, 0), table_name);
}

Table *parseInsertRequest(const char *target_file, char *table_name, char *payload) {
    if (!payload) return NULL;

    regex_t regexp_rec;
    regmatch_t groups[3];
    int comp_ec, exec_ec;

    comp_ec = regcomp(&regexp_rec, "^\\{\"cn\":\"(.*)\",\"c\":\\[(.*)\\]\\}$", REG_EXTENDED);

    if (comp_ec != REG_NOERROR) return NULL;

    exec_ec = regexec(&regexp_rec, payload, 3, groups, 0);

    if (exec_ec == REG_NOMATCH) return NULL;

    char *s_cell_number = substrToNewInstance(payload, groups[1].rm_so, groups[1].rm_eo);
    char *s_cells = substrToNewInstance(payload, groups[2].rm_so, groups[2].rm_eo);
    size_t num_of_cells = string_to_size_t(s_cell_number);

    regfree(&regexp_rec);


    char *cell_reg = "\\{\"n\":\"(.*)\",\"v\":\"(.*)\"\\},";
    size_t offset = 1;

    char *fields_format = (char*) malloc(strlen(cell_reg) * num_of_cells + 2);
    fields_format[0] = '^';
    for (size_t i = 0; i < num_of_cells; i++, offset += strlen(cell_reg)) memcpy(fields_format + offset, cell_reg, strlen(cell_reg) + 1);
    fields_format[offset - 1] = '$';
    fields_format[offset] = '\0';

    regex_t regexp_fields_rec;

    regmatch_t cell_groups[2 * num_of_cells + 1];
    int comp_fields_ec, exec_fields_ec;

    comp_fields_ec = regcomp(&regexp_fields_rec, fields_format, REG_EXTENDED);

    if (comp_fields_ec != REG_NOERROR) return NULL;

    exec_fields_ec = regexec(&regexp_fields_rec, s_cells, 2 * num_of_cells + 1, cell_groups, 0);

    if (exec_fields_ec == REG_NOMATCH) return NULL;

    free(fields_format);
    regfree(&regexp_fields_rec);

    char **cells = (char**) malloc(sizeof(char*) * num_of_cells);

    TableSchema *tableSchema = getSchema(target_file, table_name);

    for (size_t i = 1; i <= num_of_cells; i++) {
        char *field_name = substrToNewInstance(s_cells, cell_groups[2 * i - 1].rm_so, cell_groups[2 * i - 1].rm_eo);
        size_t field_index;
        for (field_index = 0; field_index < tableSchema->number_of_fields; field_index++)
            if (strcmp(tableSchema->fields[field_index]->field_name, field_name) == 0) break;

        free(field_name);
        cells[field_index] = substrToNewInstance(s_cells, cell_groups[2 * i].rm_so, cell_groups[2 * i].rm_eo);
    }

    free(s_cell_number);
    free(s_cells);

    Table *table = createTable(tableSchema, table_name);
    insertTableRecord(table, createTableRecord(num_of_cells, cells));

    return table;
}

AQLServiceResponse *parse_and_execute(AQLServiceRequest *aqlServiceRequest, const char *target_file) {
    RequestHeader *requestHeader = parseRequestHeader(aqlServiceRequest->common_header);
    AQLServiceResponse *aqlServiceResponse = (AQLServiceResponse*) malloc(sizeof(AQLServiceResponse));

    switch (requestHeader->type) {
        case 0: {
            // Parse and execute as create
            Table *table = parseCreateRequest(requestHeader->table, aqlServiceRequest->payload);
            if (addTableHeader(target_file, table) == 0) {
                aqlServiceResponse->error = createDataCell("");
                aqlServiceResponse->status = createDataCell("OK");
                aqlServiceResponse->payload = createDataCell("Table successfully created");
            } else {
                aqlServiceResponse->error = createDataCell("Such table already exists");
                aqlServiceResponse->status = createDataCell("ERROR");
                aqlServiceResponse->payload = createDataCell("");
            }
            destroyTable(table);
            break;
        }
        case 1: {
            // Parse and execute as select (might be joined)
            break;
        }
        case 2: {
            // Parse and execute as insert
            Table *table = parseInsertRequest(target_file, requestHeader->table, aqlServiceRequest->payload);
            if (insertTableRecords(target_file, table) == 0) {
                aqlServiceResponse->error = createDataCell("");
                aqlServiceResponse->status = createDataCell("OK");
                aqlServiceResponse->payload = createDataCell("Insertion executed successfully");
            } else {
                aqlServiceResponse->error = createDataCell("Invalid table name");
                aqlServiceResponse->status = createDataCell("ERROR");
                aqlServiceResponse->payload = createDataCell("");
            }
            destroyTable(table);
            break;
        }
        case 3: {
            // Parse and execute as update
            break;
        }
        case 4: {
            // Parse and execute as delete
            break;
        }
        case 5: {
            // Parse and execute as drop (empty request body)
            if (deleteTable(target_file, requestHeader->table) == 0) {
                aqlServiceResponse->error = createDataCell("");
                aqlServiceResponse->status = createDataCell("OK");
                aqlServiceResponse->payload = createDataCell("Table dropped successfully");
            } else {
                aqlServiceResponse->error = createDataCell("Invalid table name");
                aqlServiceResponse->status = createDataCell("ERROR");
                aqlServiceResponse->payload = createDataCell("");
            }
            break;
        }
    }

    return aqlServiceResponse;
}

int server(const char *url, const char *target_file)
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

    // DB is recreated every time the server is turned on
    freeDatabaseFile(target_file);
    createDatabasePage(target_file, "Test Database");

    for (;;) {
		char aql_query[32768];
		int rc;

        rc = nn_recv(fd, aql_query, sizeof(aql_query), 0);
        if (rc < 0) {
            fprintf (stderr, "nn_recv: %s\n", nn_strerror(nn_errno()));
            break;
        }

		AQLServiceRequest *aql_request = aqlservice_request__unpack(NULL, rc, aql_query);

		if (aql_request == NULL) aql_log(MSG_OUT, "NULL");
        else {
            if (rc < strlen(aql_request->payload)) {
                aql_request->payload[rc] = '\0';
            } else {
                aql_request->payload[strlen(aql_request->payload)] = '\0';
            }

            aql_log(MSG_IN, aql_request->common_header);
            aql_log(MSG_IN, aql_request->payload);

            AQLServiceResponse *response = parse_and_execute(aql_request, target_file);

            aqlservice_request__free_unpacked(aql_request, NULL);

            response_packed = pack_aql_response(response->payload, response->status, response->error);

            free(response->error);
            free(response->status);
            free(response->payload);
            free(response);

            if (nn_send(fd, response_packed.payload, response_packed.size, 0) < 0) {
                fprintf(stderr, "nn_send: %s (ignored)\n", nn_strerror(nn_errno()));
                nn_close(fd);
            }

            aql_log(MSG_OUT, "Response sent");

            if (response_packed.payload) free(response_packed.payload);
        }
    }

    nn_close (fd);
    return 0;
}

int main(int argc, char** argv) {
    if (argc != 3) {
        printf("Invalid arguments\nAn address (example: tcp://127.0.0.1:7777) and a file path should be passed");
        return -1;
    }

	int rc = server(argv[1], argv[2]);
	exit(rc == 0 ? EXIT_SUCCESS : EXIT_FAILURE);
}
