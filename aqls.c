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
#define BUFFER_SIZE 32768

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

    TableSchema *tableSchema = getSchema(target_file, table_name);

    char **cells = (char**) malloc(sizeof(char*) * num_of_cells);

    if (tableSchema == NULL || tableSchema->number_of_fields != num_of_cells) {
        free(s_cell_number);
        free(s_cells);
        return NULL;
    }

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

literal *parseLiteral(char *serialized) {
    if (!serialized) return NULL;
    char *format = (char*) malloc(strlen("^\\{'T':'(.)','V':'(.+?)'\\}$") + 1);
    memcpy(format, "^\\{\"T\":\"(.)\",\"V\":\"(.+?)\"\\}$", strlen("^\\{\"T\":\"(.)\",\"V\":\"(.+?)\"\\}$"));
    format[strlen("^\\{\"T\":\"(.)\",\"V\":\"(.+?)\"\\}$")] = 0;

    regex_t regexp_rec;
    regmatch_t groups[3];
    int comp_ec, exec_ec;

    comp_ec = regcomp(&regexp_rec, format, REG_EXTENDED);

    if (comp_ec != REG_NOERROR) return NULL;

    exec_ec = regexec(&regexp_rec, serialized, 3, groups, 0);

    if (exec_ec == REG_NOMATCH) return NULL;

    literal *lit = (literal*) malloc(sizeof(literal));
    lit->type = serialized[groups[1].rm_so] - '0';
    lit->value = substrToNewInstance(serialized, groups[2].rm_so, groups[2].rm_eo);

    free(format);
    regfree(&regexp_rec);

    return lit;
}

reference *parseReference(char *serialized) {
    if (!serialized) return NULL;
    char *format = (char*) malloc(strlen("^\\{'T':'(.+?)','F':'(.+?)'\\}$") + 1);
    memcpy(format, "^\\{\"T\":\"(.+?)\",\"F\":\"(.+?)\"\\}$", strlen("^\\{\"T\":\"(.+?)\",\"F\":\"(.+?)\"\\}$"));
    format[strlen("^\\{\"T\":\"(.+?)\",\"F\":\"(.+?)\"\\}$")] = 0;

    regex_t regexp_rec;
    regmatch_t groups[3];
    int comp_ec, exec_ec;

    comp_ec = regcomp(&regexp_rec, format, REG_EXTENDED);

    if (comp_ec != REG_NOERROR) return NULL;

    exec_ec = regexec(&regexp_rec, serialized, 3, groups, 0);

    if (exec_ec == REG_NOMATCH) return NULL;

    reference *ref = (reference*) malloc(sizeof(reference));
    ref->table = substrToNewInstance(serialized, groups[1].rm_so, groups[1].rm_eo);
    ref->field = substrToNewInstance(serialized, groups[2].rm_so, groups[2].rm_eo);

    free(format);
    regfree(&regexp_rec);

    return ref;
}

typedef struct {
    char *left;
    char *right;
} PredicateBranches;

PredicateBranches *separateBranches(char *serialized) {
    if (!serialized) return NULL;

    size_t br_count = 1, l_so = 4, l_eo, r_so, r_eo = strlen(serialized);

    for (l_eo = l_so + 1; br_count != 0 && l_eo < strlen(serialized); l_eo++) {
        if (serialized[l_eo] == '{') br_count++;
        if (serialized[l_eo] == '}') br_count--;
    }

    r_so = l_eo + 5;

    PredicateBranches *branches = (PredicateBranches*) malloc(sizeof(PredicateBranches));
    branches->left = substrToNewInstance(serialized, l_so, l_eo);
    branches->right = substrToNewInstance(serialized, r_so, r_eo);
    return branches;
}

predicate *parsePredicate(char *serialized) {
    if (!serialized) return NULL;
    char *format = "^\\{\"lt\":\"(.)\",\"rt\":\"(.)\",\"ct\":\"(.)\",\"ot\":\"(.)\",\"p\":\"(.)\",(.+?)\\}$";

    // 1 - l_type
    // 2 - r_type
    // 3 - cmp_type
    // 4 - op_type
    // 5 - priority
    // 6 - branches

    regex_t regexp_rec;
    regmatch_t groups[7];
    int comp_ec, exec_ec;

    comp_ec = regcomp(&regexp_rec, format, REG_EXTENDED);

    if (comp_ec != REG_NOERROR) return NULL;

    exec_ec = regexec(&regexp_rec, serialized, 7, groups, 0);

    if (exec_ec == REG_NOMATCH) return NULL;

    predicate *pred = (predicate*) malloc(sizeof(predicate));

    pred->l_type = serialized[groups[1].rm_so] - '0';
    pred->r_type = serialized[groups[2].rm_so] - '0';
    pred->cmp_type = serialized[groups[3].rm_so] - '0';
    pred->op_type = serialized[groups[4].rm_so] - '0';
    pred->priority = serialized[groups[5].rm_so] - '0';

    regfree(&regexp_rec);

    char *str_branches = substrToNewInstance(serialized, groups[6].rm_so, groups[6].rm_eo);
    PredicateBranches *branches = separateBranches(str_branches);

    switch (pred->l_type) {
        case 0:
            // predicate
            pred->left = parsePredicate(branches->left);
            break;
        case 1:
            // reference
            pred->l_ref = parseReference(branches->left);
            break;
        case 2:
            // literal
            pred->l_lit = parseLiteral(branches->left);
            break;
    }

    switch (pred->r_type) {
        case 0:
            // predicate
            pred->right = parsePredicate(branches->right);
            break;
        case 1:
            // reference
            pred->r_ref = parseReference(branches->right);
            break;
        case 2:
            // literal
            pred->r_lit = parseLiteral(branches->right);
            break;
    }

    free(branches->left);
    free(branches->right);
    free(branches);
    free(str_branches);

    return pred;
}

int parseDeleteRequest(const char *target_file, char *table_name, char *payload) {
    if (!target_file || !table_name || !payload) return -1;

    regex_t regexp_rec;
    regmatch_t groups[2];
    int comp_ec, exec_ec;

    comp_ec = regcomp(&regexp_rec, "^\\{\"vn\":\".*\",\"v\":\\[.*\\],\"pr\":(.*)\\}$", REG_EXTENDED);

    if (comp_ec != REG_NOERROR) return -1;

    exec_ec = regexec(&regexp_rec, payload, 2, groups, 0);

    if (exec_ec == REG_NOMATCH) return -1;

    char *str_pred = substrToNewInstance(payload, groups[1].rm_so, groups[1].rm_eo);
    predicate *pred = parsePredicate(str_pred);
    free(str_pred);

    regfree(&regexp_rec);

    int removed = deleteRowsPred(target_file, table_name, pred);

    free_predicate(pred);

    return removed;
}

int parseUpdateRequest(const char *target_file, char *table_name, char *payload) {
    if (!payload) return -1;

    regex_t regexp_rec;
    regmatch_t groups[4];
    int comp_ec, exec_ec;

    comp_ec = regcomp(&regexp_rec, "^\\{\"vn\":\".*\",\"v\":\\[.*\\],\"cn\":\"(.*)\",\"c\":\\[(.*)\\],\"pr\":(.*)\\}$", REG_EXTENDED);

    if (comp_ec != REG_NOERROR) return -1;

    exec_ec = regexec(&regexp_rec, payload, 4, groups, 0);

    if (exec_ec == REG_NOMATCH) return -1;

    char *s_cell_number = substrToNewInstance(payload, groups[1].rm_so, groups[1].rm_eo);
    char *s_cells = substrToNewInstance(payload, groups[2].rm_so, groups[2].rm_eo);
    char *s_pred = substrToNewInstance(payload, groups[3].rm_so, groups[3].rm_eo);
    size_t num_of_cells = string_to_size_t(s_cell_number);
    free(s_cell_number);

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

    if (comp_fields_ec != REG_NOERROR) return -1;

    exec_fields_ec = regexec(&regexp_fields_rec, s_cells, 2 * num_of_cells + 1, cell_groups, 0);

    if (exec_fields_ec == REG_NOMATCH) return -1;

    free(fields_format);
    regfree(&regexp_fields_rec);

    TableSchema *tableSchema = getSchema(target_file, table_name);

    char **cells = (char**) malloc(sizeof(char*) * tableSchema->number_of_fields);
    memset(cells, 0, sizeof(char*) * tableSchema->number_of_fields);

    if (tableSchema == NULL) {
        free(s_cells);
        return -1;
    }

    for (size_t i = 1; i <= num_of_cells; i++) {
        char *field_name = substrToNewInstance(s_cells, cell_groups[2 * i - 1].rm_so, cell_groups[2 * i - 1].rm_eo);
        size_t field_index;
        for (field_index = 0; field_index < tableSchema->number_of_fields; field_index++)
            if (strcmp(tableSchema->fields[field_index]->field_name, field_name) == 0) break;

        free(field_name);
        cells[field_index] = substrToNewInstance(s_cells, cell_groups[2 * i].rm_so, cell_groups[2 * i].rm_eo);
    }

    free(s_cells);

    TableRecord *tableRecord = createTableRecord(tableSchema->number_of_fields, cells);

    predicate *pred = parsePredicate(s_pred);
    free(s_pred);

    int exitcode = updateRowsPred(target_file, table_name, tableRecord, pred);

    destroyTableRecord(tableRecord);
    free_predicate(pred);

    return exitcode;
}

typedef struct {
    table_var_link *first;
    table_var_link *second;
} VarPair;

table_var_link *parseTableVarLink(const char *serialized) {
    if (!serialized) return NULL;
    char *format = "^\\{\"t\":\"(.+?)\",\"v\":\"(.+?)\"\\}$";

    regex_t regexp_rec;
    regmatch_t groups[3];
    int comp_ec, exec_ec;

    comp_ec = regcomp(&regexp_rec, format, REG_EXTENDED);

    if (comp_ec != REG_NOERROR) return NULL;

    exec_ec = regexec(&regexp_rec, serialized, 3, groups, 0);

    if (exec_ec == REG_NOMATCH) return NULL;

    table_var_link *table_var = (table_var_link*) malloc(sizeof(table_var_link));
    table_var->table = substrToNewInstance(serialized, groups[1].rm_so, groups[1].rm_eo);
    table_var->var = substrToNewInstance(serialized, groups[2].rm_so, groups[2].rm_eo);

    regfree(&regexp_rec);

    return table_var;
}

VarPair *parseVariables(const char *serialized, size_t vn) {
    if (!serialized || vn > 2) return NULL;
    VarPair *varPair = (VarPair*) malloc(sizeof(VarPair));
    varPair->first = NULL;
    varPair->second = NULL;

    if (vn == 1) varPair->first = parseTableVarLink(serialized);
    else {
        char *format = "^\\{\"t\":\"(.+?)\",\"v\":\"(.+?)\"\\},\\{\"t\":\"(.+?)\",\"v\":\"(.+?)\"\\}$";

        regex_t regexp_rec;
        regmatch_t groups[5];
        int comp_ec, exec_ec;

        comp_ec = regcomp(&regexp_rec, format, REG_EXTENDED);

        if (comp_ec != REG_NOERROR) return NULL;

        exec_ec = regexec(&regexp_rec, serialized, 5, groups, 0);

        if (exec_ec == REG_NOMATCH) return NULL;

        varPair->first = (table_var_link*) malloc(sizeof(table_var_link));
        varPair->first->table = substrToNewInstance(serialized, groups[1].rm_so, groups[1].rm_eo);
        varPair->first->var = substrToNewInstance(serialized, groups[2].rm_so, groups[2].rm_eo);
        varPair->second->table = substrToNewInstance(serialized, groups[3].rm_so, groups[3].rm_eo);
        varPair->second->var = substrToNewInstance(serialized, groups[4].rm_so, groups[4].rm_eo);

        regfree(&regexp_rec);
    }

    return varPair;
}

typedef struct {
    char *message;
    int code;
} SelectExitcode;

SelectExitcode *tableToString(Table *table) {
    if (!table) {
        SelectExitcode *selectExitcode = (SelectExitcode*) malloc(sizeof(SelectExitcode));
        selectExitcode->code = -1;
        selectExitcode->message = createDataCell("Internal server error");
        return selectExitcode;
    }

    SelectExitcode *selectExitcode = (SelectExitcode*) malloc(sizeof(SelectExitcode));
    selectExitcode->code = 0;

    selectExitcode->message = (char*) malloc(BUFFER_SIZE);
    memset(selectExitcode->message, 0, BUFFER_SIZE);
    selectExitcode->message[0] = '\n';
    selectExitcode->message[1] = '\n';

    for (size_t i = 0; i < table->tableSchema->number_of_fields; i++) {
        strcat(selectExitcode->message, "\t");
        strcat(selectExitcode->message, table->tableSchema->fields[i]->field_name);
        strcat(selectExitcode->message, "\t:\t");
        switch (table->tableSchema->fields[i]->fieldType) {
            case INTEGER_F:
                strcat(selectExitcode->message, "INTEGER");
                break;
            case FLOAT_F:
                strcat(selectExitcode->message, "FLOAT");
                break;
            case BOOLEAN_F:
                strcat(selectExitcode->message, "BOOLEAN");
                break;
            default:
                strcat(selectExitcode->message, "STRING");
                break;
        }
        if (i < table->tableSchema->number_of_fields - 1) strcat(selectExitcode->message, "\t|");
    }

    strcat(selectExitcode->message, "\n\n\n\n");

    TableRecord *tableRecord = table->firstTableRecord;
    for (size_t i = 0; i < table->length; i++, tableRecord = tableRecord->next_record) {
        strcat(selectExitcode->message, "\t");
        for (size_t j = 0; j < table->tableSchema->number_of_fields; j++) {
            strcat(selectExitcode->message, table->tableSchema->fields[j]->field_name);
            strcat(selectExitcode->message, " :\t");
            strcat(selectExitcode->message, tableRecord->dataCells[j]);
            if (j < table->tableSchema->number_of_fields - 1) strcat(selectExitcode->message, "\t|\t");
        }
        strcat(selectExitcode->message, "\n");
    }

    strcat(selectExitcode->message, "\n");

    return selectExitcode;
}

reference **parseFieldNames(size_t rn, const char *serialized) {
    if (rn == 0 || !serialized) return NULL;

    char *field_reg = "\\{\"t\":\"(.*)\",\"f\":\"(.*)\"\\},";
    size_t offset = 1;

    char *fields_format = (char*) malloc(strlen(field_reg) * rn + 2);
    fields_format[0] = '^';
    for (size_t i = 0; i < rn; i++, offset += strlen(field_reg)) memcpy(fields_format + offset, field_reg, strlen(field_reg) + 1);
    fields_format[offset - 1] = '$';
    fields_format[offset] = '\0';

    regex_t regexp_fields_rec;

    regmatch_t field_groups[2 * rn + 1];
    int comp_fields_ec, exec_fields_ec;

    comp_fields_ec = regcomp(&regexp_fields_rec, fields_format, REG_EXTENDED);

    if (comp_fields_ec != REG_NOERROR) return NULL;

    exec_fields_ec = regexec(&regexp_fields_rec, serialized, 2 * rn + 1, field_groups, 0);

    if (exec_fields_ec == REG_NOMATCH) return NULL;

    free(fields_format);

    reference **refs = (reference**) malloc(sizeof(reference*) * rn);

    for (size_t i = 1; i <= rn; i++) {
        refs[i - 1] = (reference*) malloc(sizeof(reference));
        refs[i - 1]->table = substrToNewInstance(serialized, field_groups[2 * i - 1].rm_so, field_groups[2 * i - 1].rm_eo);
        refs[i - 1]->field = substrToNewInstance(serialized, field_groups[2 * i].rm_so, field_groups[2 * i].rm_eo);
    }

    regfree(&regexp_fields_rec);

    return refs;
}

SelectExitcode *parseSelectRequest(const char *target_file, const char *payload) {
    if (!target_file || !payload) {
        SelectExitcode *selectExitcode = (SelectExitcode*) malloc(sizeof(SelectExitcode));
        selectExitcode->code = -1;
        selectExitcode->message = createDataCell("Internal server error");
        return selectExitcode;
    }

    char *format = "^\\{\"st\":\"(.)\",\"vn\":\"(.)\",\"v\":\\[(.+?)\\],\"rn\":\"(.+?)\",\"r\":\\[(.*)\\],\"pr\":(.*)\\}$";

    regex_t regexp_rec;
    regmatch_t groups[7];
    int comp_ec, exec_ec;

    comp_ec = regcomp(&regexp_rec, format, REG_EXTENDED);

    if (comp_ec != REG_NOERROR) {
        SelectExitcode *selectExitcode = (SelectExitcode*) malloc(sizeof(SelectExitcode));
        selectExitcode->code = -1;
        selectExitcode->message = createDataCell("Regex error");
        return selectExitcode;
    }

    exec_ec = regexec(&regexp_rec, payload, 7, groups, 0);

    if (exec_ec == REG_NOMATCH) {
        SelectExitcode *selectExitcode = (SelectExitcode*) malloc(sizeof(SelectExitcode));
        selectExitcode->code = -1;
        selectExitcode->message = createDataCell("Regex: no match");
        return selectExitcode;
    }

    size_t vn = payload[groups[2].rm_so] - '0';
    char *s_vars = substrToNewInstance(payload, groups[3].rm_so, groups[3].rm_eo);
    VarPair *varPair = parseVariables(s_vars, vn);
    free(s_vars);

    char *s_rn = substrToNewInstance(payload, groups[4].rm_so, groups[4].rm_eo);
    char *s_fields = substrToNewInstance(payload, groups[5].rm_so, groups[5].rm_eo);
    size_t rn = string_to_size_t(s_rn);
    free(s_rn);

    reference **refs = parseFieldNames(rn, s_fields);
    char **field_names = NULL;

    if (refs) {
        field_names = (char**) malloc(sizeof(char*) * rn);
        for (size_t i = 0; i < rn; i++) field_names[i] = createDataCell(refs[i]->field);
    }

    switch (payload[groups[1].rm_so] - '0') {
        case 0: {
            // Unfiltered
            Table *table = filteredSelect(target_file, varPair->first->table, varPair->first->var, NULL, rn, field_names);
            SelectExitcode *selectExitcode = tableToString(table);

            destroyTable(table);
            free(varPair->first->table);
            free(varPair->first->var);
            free(varPair->first);

            return selectExitcode;
        }
        case 1: {
            // Filtered
            char *s_pred = substrToNewInstance(payload, groups[6].rm_so, groups[6].rm_eo);
            predicate *pred = parsePredicate(s_pred);
            free(s_pred);
            Table *table = filteredSelect(target_file, varPair->first->table, varPair->first->var, pred, rn, field_names);
            SelectExitcode *selectExitcode = tableToString(table);

            destroyTable(table);
            free_predicate(pred);
            free(varPair->first->table);
            free(varPair->first->var);
            free(varPair->first);

            return selectExitcode;
        }
        case 2: {
            // Joined
            char *s_pred = substrToNewInstance(payload, groups[6].rm_so, groups[6].rm_eo);
            predicate *pred = parsePredicate(s_pred);
            free(s_pred);
            Table *table = joinedSelect(target_file, varPair->first->table, varPair->first->var, varPair->second->table, varPair->second->var, pred, rn, refs);
            SelectExitcode *selectExitcode = tableToString(table);

            destroyTable(table);
            free_predicate(pred);
            free(varPair->first->table);
            free(varPair->first->var);
            free(varPair->first);
            free(varPair->second->table);
            free(varPair->second->var);
            free(varPair->second);

            return selectExitcode;
        }
    }

    if (refs) {
        for (size_t i = 0; i < rn; i++) free(field_names[i]);
        free(field_names);
        for (size_t i = 0; i < rn; i++) {
            free(refs[i]->field);
            free(refs[i]->table);
            free(refs[i]);
        }
        free(refs);
    }

    regfree(&regexp_rec);
    free(varPair);

    SelectExitcode *selectExitcode = (SelectExitcode*) malloc(sizeof(SelectExitcode));
    selectExitcode->code = -1;
    selectExitcode->message = createDataCell("Internal server error");
    return selectExitcode;
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
            SelectExitcode *selectExitcode = parseSelectRequest(target_file, aqlServiceRequest->payload);
            if (selectExitcode->code == 0) {
                aqlServiceResponse->error = createDataCell("");
                aqlServiceResponse->status = createDataCell("OK");
                aqlServiceResponse->payload = createDataCell(selectExitcode->message);
            } else {
                aqlServiceResponse->error = createDataCell(selectExitcode->message);
                aqlServiceResponse->status = createDataCell("ERROR");
                aqlServiceResponse->payload = createDataCell("");
            }
            free(selectExitcode->message);
            free(selectExitcode);
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
            if (parseUpdateRequest(target_file, requestHeader->table, aqlServiceRequest->payload) == 0) {
                aqlServiceResponse->error = createDataCell("");
                aqlServiceResponse->status = createDataCell("OK");
                aqlServiceResponse->payload = createDataCell("Rows updated successfully");
            } else {
                aqlServiceResponse->error = createDataCell("Invalid table name");
                aqlServiceResponse->status = createDataCell("ERROR");
                aqlServiceResponse->payload = createDataCell("");
            }
            break;
        }
        case 4: {
            // Parse and execute as delete
            if (parseDeleteRequest(target_file, requestHeader->table, aqlServiceRequest->payload) >= 0) {
                aqlServiceResponse->error = createDataCell("");
                aqlServiceResponse->status = createDataCell("OK");
                aqlServiceResponse->payload = createDataCell("Rows removed successfully");
            } else {
                aqlServiceResponse->error = createDataCell("Invalid table name");
                aqlServiceResponse->status = createDataCell("ERROR");
                aqlServiceResponse->payload = createDataCell("");
            }
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

    printf("Server is up\n");

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