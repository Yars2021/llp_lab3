//
// Created by yars on 30.09.22.
//

#ifndef LLP_LAB1_C_DB_INTERNALS_H
#define LLP_LAB1_C_DB_INTERNALS_H

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <regex.h>

#define JSON_FIELD_NAME "F_NAME"
#define JSON_FIELD_TYPE "F_TYPE"
#define JSON_SCHEMA_KEY_I "KEY_COL_I"
#define JSON_SCHEMA_NUM_OF_FIELDS "NUM_OF_FIELDS"
#define JSON_SCHEMA_FIELDS "FIELDS"
#define JSON_RECORD "RECORD"
#define JSON_TABLE_NAME "T_NAME"
#define JSON_TABLE_LINK "T_LINK"

/// Types of fields in a table.
typedef enum {
    INTEGER_F,
    FLOAT_F,
    STRING_F,
    BOOLEAN_F
} FieldType;


/// Table field.
typedef struct {
    char *field_name;
    FieldType fieldType;
} Field;

/// Converts a string into an unsigned int.
size_t string_to_size_t(char *line);

/// Creates a new instance of a field.
Field *createField(char *field_name, FieldType fieldType);

/// Returns a valid JSON string containing the field data.
char *transformFieldToJSON(Field *field);

/// Creates a field from a JSON line.
/// Parses from pos and saves the ending index.
Field *parseFieldJSON(const char *line, size_t pos, size_t *ending_index);

/// Destroys the field instance.
void destroyField(Field *field);


/// Describes the table structure (column types and names as an array of Field pointers and the index of the ID column).
typedef struct {
    size_t key_column_index;
    size_t number_of_fields;
    Field **fields;
} TableSchema;

/// Creates a new instance of a schema.
TableSchema *createTableSchema(Field **fields, size_t number_of_fields, size_t key_column_index);

/// Returns a valid JSON string containing the schema data.
char *transformTableSchemaToJSON(TableSchema *tableSchema);

/// Creates a table schema from a JSON line.
/// Parses from pos and saves the ending index.
TableSchema *parseTableSchemaJSON(const char *line, size_t pos, size_t *ending_index);

/// Destroys the schema instance.
void destroyTableSchema(TableSchema *tableSchema);


/// Creates a table record, which is an array of string pointers.
/// Each record is linked to the next and previous ones, which helps to iterate through them.
typedef struct TableRecord {
    size_t length;
    struct TableRecord *next_record;
    struct TableRecord *prev_record;
    char **dataCells;
} TableRecord;

/// Creates a data cell from a string.
char *createDataCell(const char *value);

/// Creates a new string, which is a substring of the original one.
char *substrToNewInstance(const char *origin, size_t begin, size_t end);

/// Creates an instance of a table record (next is NULL by default).
TableRecord *createTableRecord(size_t length, char **dataCells);

/// Returns a valid JSON string containing the record data.
char *transformTableRecordToJSON(TableRecord *tableRecord);

/// Creates a table record from a JSON line following the provided TableSchema.
/// Parses from pos and saves the ending index.
TableRecord *parseTableRecordJSON(const char *line, size_t pos, size_t *ending_index, TableSchema *tableSchema);

/// destroys the instance of table record.
void destroyTableRecord(TableRecord *tableRecord);


/// Table that is described by a schema and has a name.
/// Points to all the first of the records inside it (2-way linked list).
typedef struct {
    char *table_name;
    TableSchema *tableSchema;
    size_t length;
    TableRecord *firstTableRecord;
} Table;

/// Creates a new table.
Table *createTable(TableSchema *tableSchema, char *table_name);

/// Adds a new line to the table.
void insertTableRecord(Table *table, TableRecord *tableRecord);

/// Destroys the table instance.
void destroyTable(Table *table);


/// Contains table name and its location.
typedef struct {
    char *table_name;
    size_t link;
} TableLink;

/// Creates a new instance of TableLink.
TableLink *createTableLink(char *table_name, size_t link);

/// Returns a valid JSON string containing the TableLink data.
char *transformTableLinkToJSON(TableLink *tableLink);

/// Creates a TableLink from a JSON line.
/// Parses from pos and saves the ending index.
TableLink *parseTableLinkJSON(const char *line, size_t pos, size_t *ending_index);

/// Destroys the TableLink instance.
void destroyTableLink(TableLink *tableLink, int save_name);

#endif //LLP_LAB1_C_DB_INTERNALS_H