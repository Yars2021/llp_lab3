//
// Created by yars on 22.11.22.
//

#ifndef LLP_LAB1_C_DB_INTERFACE_H
#define LLP_LAB1_C_DB_INTERFACE_H

#include <math.h>
#include "db_internals.h"
#include "../data.h"

#define FLOAT_CMP_EPS 0.0000000001
#define FILTER_REJECT 0
#define FILTER_ACCEPT 1
#define FILTER_INCOMPATIBLE (-1)
#define FILTER_NULL_POINTER (-2)

/// A filter, which binds to a table field by index.
typedef struct {
    FieldType fieldType;
    size_t field_index;
    int operation;
    char *operand;
} SearchFilter;

/// Parses a data cell into an Integer value.
int64_t parseInteger(const char *line);

/// Parses a data cell into a Float value.
double parseFloat(const char *line);

/// Creates a new instance of a SearchFilter.
SearchFilter *createSearchFilter(FieldType fieldType, int operation, char *operand);

/// Binds the filter to the column by its index.
void bindFilter(SearchFilter *searchFilter, size_t column);

/// Applies the filter to the data cell. If the type is incompatible, returns FILTER_INCOMPATIBLE.
int applyFilter(SearchFilter *searchFilter, const char *dataCell);

/// Filters a table record.
int applyAll(TableRecord *tableRecord, size_t num_of_filters, SearchFilter **filters);

/// Destroys the SearchFilter.
void destroySearchFilter(SearchFilter *searchFilter);

int invertExitcode(int exitcode);

void invertFilter(SearchFilter *searchFilter);

int applySingleTablePredicate(TableSchema *tableSchema, TableRecord *tableRecord, predicate *pred);

int applyJoinPredicate(TableSchema *leftSchema, TableSchema *rightSchema, TableRecord *tableRecord, predicate *pred);

#endif //LLP_LAB1_C_DB_INTERFACE_H
