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

SearchFilter *createSearchFilter(FieldType fieldType, void *lower_threshold, void *upper_threshold)
{
    SearchFilter *searchFilter = (SearchFilter*) malloc(sizeof(SearchFilter));
    searchFilter->fieldType = fieldType;
    searchFilter->inverted = 0;
    searchFilter->lower_threshold = lower_threshold;
    searchFilter->upper_threshold = upper_threshold;
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
    switch (searchFilter->fieldType) {
        case INTEGER_F: {
            int64_t cell_value = parseInteger(dataCell);
            if (!searchFilter->lower_threshold) {
                if (!searchFilter->upper_threshold) return FILTER_ACCEPT;
                else {
                    if (cell_value > *(int64_t*) searchFilter->upper_threshold) return FILTER_REJECT;
                    else return FILTER_ACCEPT;
                }
            } else {
                if (!searchFilter->upper_threshold) {
                    if (cell_value < *(int64_t*) searchFilter->lower_threshold) return FILTER_REJECT;
                    else return FILTER_ACCEPT;
                } else {
                    if (cell_value < *(int64_t*) searchFilter->lower_threshold || cell_value > *(int64_t*) searchFilter->upper_threshold)
                        return FILTER_REJECT;
                    else return FILTER_ACCEPT;
                }
            }
        }
        case FLOAT_F: {
            double cell_value = parseFloat(dataCell);
            if (!searchFilter->lower_threshold) {
                if (!searchFilter->upper_threshold) return FILTER_ACCEPT;
                else {
                    if ((cell_value - *(double*) searchFilter->upper_threshold) > FLOAT_CMP_EPS) return FILTER_REJECT;
                    else return FILTER_ACCEPT;
                }
            } else {
                if (!searchFilter->upper_threshold) {
                    if ((cell_value - *(double*) searchFilter->lower_threshold) < -FLOAT_CMP_EPS) return FILTER_REJECT;
                    else return FILTER_ACCEPT;
                } else {
                    if ((cell_value - *(double*) searchFilter->upper_threshold) > FLOAT_CMP_EPS || (cell_value - *(double*) searchFilter->lower_threshold) < -FLOAT_CMP_EPS)
                        return FILTER_REJECT;
                    else return FILTER_ACCEPT;
                }
            }
        }
        case BOOLEAN_F: {
            if (!searchFilter->lower_threshold) {
                if (!searchFilter->upper_threshold) return FILTER_ACCEPT;
                else {
                    if (cmpBoolean(dataCell, (char*) searchFilter->upper_threshold) > 0) return FILTER_REJECT;
                    else return FILTER_ACCEPT;
                }
            } else {
                if (!searchFilter->upper_threshold) {
                    if (cmpBoolean(dataCell, (char*) searchFilter->lower_threshold) < 0) return FILTER_REJECT;
                    else return FILTER_ACCEPT;
                } else {
                    if (cmpBoolean(dataCell, (char*) searchFilter->lower_threshold) < 0 ||
                        cmpBoolean(dataCell, (char*) searchFilter->upper_threshold) > 0)
                        return FILTER_REJECT;
                    else return FILTER_ACCEPT;
                }
            }
        }
        default: {
            if (!searchFilter->lower_threshold) {
                if (!searchFilter->upper_threshold) return FILTER_ACCEPT;
                else {
                    if (strcmp(dataCell, (char*) searchFilter->upper_threshold) > 0) return FILTER_REJECT;
                    else return FILTER_ACCEPT;
                }
            } else {
                if (!searchFilter->upper_threshold) {
                    if (strcmp(dataCell, (char*) searchFilter->lower_threshold) < 0) return FILTER_REJECT;
                    else return FILTER_ACCEPT;
                } else {
                    if (strcmp(dataCell, (char*) searchFilter->lower_threshold) < 0 || strcmp(dataCell, (char*) searchFilter->upper_threshold) > 0)
                        return FILTER_REJECT;
                    else return FILTER_ACCEPT;
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

        if (filters[i]->inverted && exitcode >= 0) exitcode = !exitcode;

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
