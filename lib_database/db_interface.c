//
// Created by yars on 22.11.22.
//

#include "db_interface.h"

int64_t parseInteger(const char *line)
{
    if (!line || strlen(line) == 0) return 0;
    int sign = 0;
    if (line[0] == '-') sign = 1;
    int64_t value = 0;
    for (size_t i = sign; i < strlen(line); i++, value *= 10) {
        if (line[i] < '0' || line[i] > '9') return 0;
        value += (line[i] - '0');
    }
    value /= 10;
    return sign ? -value : value;
}

double parseFloat(const char *line)
{
    if (!line || strlen(line) == 0) return 0.0;
    size_t pt_index = 0;
    for (size_t i = 1; (i < strlen(line) - 1) && !pt_index; i++) if (line[i] == '.') pt_index = i;
    if (!pt_index) return (double) parseInteger(line);
    char *pointless_string = (char*) malloc(strlen(line));
    for (size_t i = 0, j = 0; j < strlen(line); i++)
        if (i == pt_index) continue;
        else pointless_string[j++] = line[i];

    double result = (double) parseInteger(pointless_string);
    free(pointless_string);
    for (size_t i = 0; i < strlen(line) - pt_index - 1; i++) result /= 10;
    return result;
}

SearchFilter *createSearchFilter(FieldType fieldType, int operation, char *operand)
{
    SearchFilter *searchFilter = (SearchFilter*) malloc(sizeof(SearchFilter));
    searchFilter->fieldType = fieldType;
    searchFilter->operation = operation;
    searchFilter->operand = operand;
    return searchFilter;
}

void bindFilter(SearchFilter *searchFilter, size_t column)
{
    if (!searchFilter) return;
    searchFilter->field_index = column;
}

int cmpBoolean(const char *a, const char *b)
{
    if (strcmp(a, b) == 0) return 0;
    if (strcmp(a, "True") == 0 && strcmp(b, "False") == 0) return 1;
    return -1;
}

int applyFilter(SearchFilter *searchFilter, const char *dataCell)
{
    if (!searchFilter || !dataCell) return FILTER_NULL_POINTER;

    /*
        "==" 1
        "!=" 2
        "<>" 2
        ">"  3
        "<"  4
        ">=" 5
        "<=" 6
        "~"  7
     */

    switch (searchFilter->fieldType) {
        case INTEGER_F: {
            switch (searchFilter->operation) {
                case 1: {
                    int cell_value = parseInteger(dataCell), operand_value = parseInteger(searchFilter->operand);
                    if (cell_value == operand_value) return FILTER_ACCEPT;
                    return FILTER_REJECT;
                }
                case 2: {
                    int cell_value = parseInteger(dataCell), operand_value = parseInteger(searchFilter->operand);
                    if (cell_value != operand_value) return FILTER_ACCEPT;
                    return FILTER_REJECT;
                }
                case 3: {
                    int cell_value = parseInteger(dataCell), operand_value = parseInteger(searchFilter->operand);
                    if (cell_value > operand_value) return FILTER_ACCEPT;
                    return FILTER_REJECT;
                }
                case 4: {
                    int cell_value = parseInteger(dataCell), operand_value = parseInteger(searchFilter->operand);
                    if (cell_value < operand_value) return FILTER_ACCEPT;
                    return FILTER_REJECT;
                }
                case 5: {
                    int cell_value = parseInteger(dataCell), operand_value = parseInteger(searchFilter->operand);
                    if (cell_value >= operand_value) return FILTER_ACCEPT;
                    return FILTER_REJECT;
                }
                case 6: {
                    int cell_value = parseInteger(dataCell), operand_value = parseInteger(searchFilter->operand);
                    if (cell_value <= operand_value) return FILTER_ACCEPT;
                    return FILTER_REJECT;
                }
                default: {
                    int cell_value = parseInteger(dataCell), operand_value = parseInteger(searchFilter->operand);
                    if (cell_value == operand_value) return FILTER_ACCEPT;
                    return FILTER_REJECT;
                }
            }
        }
        case FLOAT_F: {
            switch (searchFilter->operation) {
                case 1: {
                    float cell_value = parseFloat(dataCell), operand_value = parseFloat(searchFilter->operand);
                    if (fabs(cell_value - operand_value) < FLOAT_CMP_EPS) return FILTER_ACCEPT;
                    return FILTER_REJECT;
                }
                case 2: {
                    float cell_value = parseFloat(dataCell), operand_value = parseFloat(searchFilter->operand);
                    if (fabs(cell_value - operand_value) >= FLOAT_CMP_EPS) return FILTER_ACCEPT;
                    return FILTER_REJECT;
                }
                case 3: {
                    float cell_value = parseFloat(dataCell), operand_value = parseFloat(searchFilter->operand);
                    if (cell_value > operand_value) return FILTER_ACCEPT;
                    return FILTER_REJECT;
                }
                case 4: {
                    int cell_value = parseFloat(dataCell), operand_value = parseFloat(searchFilter->operand);
                    if (cell_value < operand_value) return FILTER_ACCEPT;
                    return FILTER_REJECT;
                }
                case 5: {
                    int cell_value = parseFloat(dataCell), operand_value = parseFloat(searchFilter->operand);
                    if (cell_value >= operand_value) return FILTER_ACCEPT;
                    return FILTER_REJECT;
                }
                case 6: {
                    int cell_value = parseFloat(dataCell), operand_value = parseFloat(searchFilter->operand);
                    if (cell_value <= operand_value) return FILTER_ACCEPT;
                    return FILTER_REJECT;
                }
                default: {
                    float cell_value = parseFloat(dataCell), operand_value = parseFloat(searchFilter->operand);
                    if (fabs(cell_value - operand_value) < FLOAT_CMP_EPS) return FILTER_ACCEPT;
                    return FILTER_REJECT;
                }
            }
        }
        case BOOLEAN_F: {
            switch (searchFilter->operation) {
                case 1: {
                    if (strcmp(dataCell, searchFilter->operand) == 0) return FILTER_ACCEPT;
                    return FILTER_REJECT;
                }
                default: {
                    if (strcmp(dataCell, searchFilter->operand) == 0) return FILTER_REJECT;
                    return FILTER_ACCEPT;
                }
            }
        }
        default: {
            switch (searchFilter->operation) {
                case 1: {
                    if (strcmp(dataCell, searchFilter->operand) == 0) return FILTER_ACCEPT;
                    return FILTER_REJECT;
                }
                case 2: {
                    if (strcmp(dataCell, searchFilter->operand) == 0) return FILTER_REJECT;
                    return FILTER_ACCEPT;
                }
                case 3: {
                    if (strcmp(dataCell, searchFilter->operand) > 0) return FILTER_ACCEPT;
                    return FILTER_REJECT;
                }
                case 4: {
                    if (strcmp(dataCell, searchFilter->operand) < 0) return FILTER_ACCEPT;
                    return FILTER_REJECT;
                }
                case 5: {
                    if (strcmp(dataCell, searchFilter->operand) >= 0) return FILTER_ACCEPT;
                    return FILTER_REJECT;
                }
                case 6: {
                    if (strcmp(dataCell, searchFilter->operand) <= 0) return FILTER_ACCEPT;
                    return FILTER_REJECT;
                }
                case 7: {
                    if (strstr(dataCell, searchFilter->operand)) return FILTER_ACCEPT;
                    return FILTER_REJECT;
                }
                default: {
                    if (strcmp(dataCell, searchFilter->operand) == 0) return FILTER_ACCEPT;
                    return FILTER_REJECT;
                }
            }
        }
    }
}

int applyAll(TableRecord *tableRecord, size_t num_of_filters, SearchFilter **filters)
{
    if (!tableRecord) return FILTER_NULL_POINTER;
    if (num_of_filters == 0 || !filters) return FILTER_ACCEPT;

    for (size_t i = 0; i < num_of_filters; i++) {
        if (filters[i]->field_index > tableRecord->length) return FILTER_INCOMPATIBLE;
        int exitcode = applyFilter(filters[i], tableRecord->dataCells[filters[i]->field_index]);
        if (exitcode == FILTER_ACCEPT) continue;
        else return exitcode;
    }

    return FILTER_ACCEPT;
}

void destroySearchFilter(SearchFilter *searchFilter)
{
    if (!searchFilter) return;
    free(searchFilter);
}

int invertExitcode(int exitcode) {
    if (exitcode == FILTER_ACCEPT) return FILTER_REJECT;
    if (exitcode == FILTER_REJECT) return FILTER_ACCEPT;
    return exitcode;
}

void invertFilter(SearchFilter *searchFilter) {
    switch (searchFilter->operation) {
        case 1:
            searchFilter->operation = 2; // == -> !=
            break;
        case 2:
            searchFilter->operation = 1; // != -> ==
            break;
        case 3:
            searchFilter->operation = 6; // > -> <=
            break;
        case 4:
            searchFilter->operation = 5; // < -> >=
            break;
        case 5:
            searchFilter->operation = 4; // >= -> <
            break;
        case 6:
            searchFilter->operation = 3; // <= -> >
            break;
    }
}

int applySimplePredicate(TableSchema *tableSchema, TableRecord *tableRecord, predicate *pred) {
    if (!tableSchema || !tableRecord || !pred) return FILTER_NULL_POINTER;

    if (pred->l_type == 0 || pred->r_type == 0) return FILTER_INCOMPATIBLE;
    int exitcode;
    switch (pred->r_type) {
        case 1: {
            size_t index1, index2;
            if (strcmp(pred->l_ref->table, pred->r_ref->table) != 0) return FILTER_ACCEPT;
            for (index1 = 0; strcmp(pred->l_ref->field, tableSchema->fields[index1]->field_name) != 0 && index1 < tableSchema->number_of_fields; index1++);
            for (index2 = 0; strcmp(pred->r_ref->field, tableSchema->fields[index2]->field_name) != 0 && index2 < tableSchema->number_of_fields; index2++);
            SearchFilter *filter = createSearchFilter(tableSchema->fields[index1]->fieldType, pred->cmp_type,
                                                      tableRecord->dataCells[index2]);
            bindFilter(filter, index1);

            exitcode = applyFilter(filter, tableRecord->dataCells[index1]);
            destroySearchFilter(filter);
            return exitcode;
        }
        case 2: {
            size_t index;
            for (index = 0; strcmp(pred->l_ref->field, tableSchema->fields[index]->field_name) != 0 &&
                            index < tableSchema->number_of_fields; index++);
            SearchFilter *filter = createSearchFilter(tableSchema->fields[index]->fieldType, pred->cmp_type,
                                                      pred->r_lit->value);
            bindFilter(filter, index);

            exitcode = applyFilter(filter, tableRecord->dataCells[index]);
            destroySearchFilter(filter);
        }
    }

    return exitcode;
}

int applySingleTablePredicate(TableSchema *tableSchema, TableRecord *tableRecord, predicate *pred) {
    if (!tableSchema || !tableRecord) return FILTER_NULL_POINTER;
    if (!pred) return FILTER_ACCEPT;
    if (pred->l_type != 0 && pred->r_type != 0) return applySimplePredicate(tableSchema, tableRecord, pred);

    int left_res, right_res;
    if (pred->priority == 0) {
        left_res = applySingleTablePredicate(tableSchema, tableRecord, pred->left);
        right_res = applySingleTablePredicate(tableSchema, tableRecord, pred->right);
    } else {
        right_res = applySingleTablePredicate(tableSchema, tableRecord, pred->right);
        left_res = applySingleTablePredicate(tableSchema, tableRecord, pred->left);
    }

    if (pred->op_type == 1) return left_res || right_res;
    return left_res && right_res;
}

int applySimpleVarPredicate(TableSchema *tableSchema, const char *var, TableRecord *tableRecord, predicate *pred) {
    if (!tableSchema || !tableRecord || !pred) return FILTER_NULL_POINTER;

    if (pred->l_type == 0 || pred->r_type == 0) return FILTER_INCOMPATIBLE;
    if (pred->l_type == 1 && strcmp(pred->l_ref->table, var) != 0) return FILTER_ACCEPT;
    int exitcode;
    switch (pred->r_type) {
        case 1: {
            size_t index1, index2;
            if (strcmp(pred->l_ref->table, pred->r_ref->table) != 0) return FILTER_ACCEPT;
            for (index1 = 0; strcmp(pred->l_ref->field, tableSchema->fields[index1]->field_name) != 0 && index1 < tableSchema->number_of_fields; index1++);
            for (index2 = 0; strcmp(pred->r_ref->field, tableSchema->fields[index2]->field_name) != 0 && index2 < tableSchema->number_of_fields; index2++);
            SearchFilter *filter = createSearchFilter(tableSchema->fields[index1]->fieldType, pred->cmp_type,
                                                      tableRecord->dataCells[index2]);
            bindFilter(filter, index1);

            exitcode = applyFilter(filter, tableRecord->dataCells[index1]);
            destroySearchFilter(filter);
            return exitcode;
        }
        case 2: {
            size_t index;
            for (index = 0; strcmp(pred->l_ref->field, tableSchema->fields[index]->field_name) != 0 &&
                            index < tableSchema->number_of_fields; index++);
            SearchFilter *filter = createSearchFilter(tableSchema->fields[index]->fieldType, pred->cmp_type,
                                                      pred->r_lit->value);
            bindFilter(filter, index);

            exitcode = applyFilter(filter, tableRecord->dataCells[index]);
            destroySearchFilter(filter);
        }
    }

    return exitcode;
}

int applyVarTablePredicate(TableSchema *tableSchema, TableRecord *tableRecord, const char *var, predicate *pred) {
    if (!tableSchema || !tableRecord || !var) return FILTER_NULL_POINTER;
    if (!pred) return FILTER_ACCEPT;
    if (pred->l_type != 0 && pred->r_type != 0) return applySimpleVarPredicate(tableSchema, var, tableRecord, pred);

    int left_res, right_res;
    if (pred->priority == 0) {
        left_res = applyVarTablePredicate(tableSchema, tableRecord, var, pred->left);
        right_res = applyVarTablePredicate(tableSchema, tableRecord, var, pred->right);
    } else {
        right_res = applyVarTablePredicate(tableSchema, tableRecord, var, pred->right);
        left_res = applyVarTablePredicate(tableSchema, tableRecord, var, pred->left);
    }

    if (pred->op_type == 1) return left_res || right_res;
    return left_res && right_res;
}

JoinIndexes *findJoinIndexes(TableSchema *leftSchema, TableSchema *rightSchema, const char *left_var, const char *right_var, predicate *pred) {
    if (!left_var || !right_var || !pred) return NULL;
    if (pred->l_type == 2 || pred->r_type == 2) return NULL;

    JoinIndexes *joinIndexes;

    if (pred->l_type == 1 && pred->r_type == 1) {
        size_t index1, index2;
        if (strcmp(pred->l_ref->table, pred->r_ref->table) == 0) return NULL;
        for (index1 = 0; strcmp(pred->l_ref->field, leftSchema->fields[index1]->field_name) != 0 && index1 < leftSchema->number_of_fields; index1++);
        for (index2 = 0; strcmp(pred->r_ref->field, rightSchema->fields[index2]->field_name) != 0 && index2 < rightSchema->number_of_fields; index2++);

        if (strcmp(pred->l_ref->field, leftSchema->fields[index1]->field_name) != 0) return NULL;
        if (strcmp(pred->r_ref->field, rightSchema->fields[index2]->field_name) != 0) return NULL;

        joinIndexes = (JoinIndexes*) malloc(sizeof(JoinIndexes));
        joinIndexes->left = index1;
        joinIndexes->right = index2;

        return joinIndexes;
    }

    if (pred->l_type == 0 && pred->r_type == 0) {
        joinIndexes = findJoinIndexes(leftSchema, rightSchema, left_var, right_var, pred->left);
        if (joinIndexes != NULL) return joinIndexes;
        joinIndexes = findJoinIndexes(leftSchema, rightSchema, left_var, right_var, pred->right);
        return joinIndexes;
    }

    return NULL;
}