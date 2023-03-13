//
// Created by yars on 30.09.22.
//
#include "db_internals.h"

size_t string_to_size_t(char *line)
{
    if (!line) return 0;
    size_t num = 0;
    for (size_t i = 0; i < strlen(line); i++, num *= 10) num += (line[i] - '0');
    return num / 10;
}

Field *createField(char *field_name, FieldType fieldType)
{
    Field *field = (Field*) malloc(sizeof(Field));
    field->field_name = field_name;
    field->fieldType = fieldType;
    return field;
}

char *transformFieldToJSON(Field *field)
{
    /// {"F_NAME":"","F_TYPE":""}

    if (!field) return "";
    char type, *line = (char*) malloc(strlen("{'':'','':''}") + strlen(JSON_FIELD_NAME) + strlen(JSON_FIELD_TYPE) + strlen(field->field_name) + 2);
    switch (field->fieldType) {
        case INTEGER_F:
            type = 'I';
            break;
        case FLOAT_F:
            type = 'F';
            break;
        case BOOLEAN_F:
            type = 'B';
            break;
        default:
            type = 'S';
            break;
    }
    sprintf(line, "{\"%s\":\"%s\",\"%s\":\"%c\"}", JSON_FIELD_NAME, field->field_name, JSON_FIELD_TYPE, type);

    return line;
}

Field *parseFieldJSON(const char *line, size_t pos, size_t *ending_index)
{
    if (!line || pos >= strlen(line)) return NULL;

    char *field_name_reg = "(.+?)", *field_type_reg = "(.)";
    char *format = (char*) malloc(strlen("^\\{'':'(.+?)','':'(.)'\\}$") + strlen(JSON_FIELD_NAME) + strlen(JSON_FIELD_TYPE) + 1);
    sprintf(format, "^\\{\"%s\":\"%s\",\"%s\":\"%s\"\\}$", JSON_FIELD_NAME, field_name_reg, JSON_FIELD_TYPE, field_type_reg);

    regex_t regexp_rec;
    regmatch_t groups[3];
    int comp_ec, exec_ec;

    comp_ec = regcomp(&regexp_rec, format, REG_EXTENDED);

    if (comp_ec != REG_NOERROR) return NULL;

    exec_ec = regexec(&regexp_rec, line + pos, 3, groups, 0);

    if (exec_ec == REG_NOMATCH) return NULL;

    Field *field = createField(substrToNewInstance(line, groups[1].rm_so, groups[1].rm_eo), STRING_F);

    switch (line[groups[2].rm_so]) {
        case 'I':
            field->fieldType = INTEGER_F;
            break;
        case 'F':
            field->fieldType = FLOAT_F;
            break;
        case 'B':
            field->fieldType = BOOLEAN_F;
            break;
        default:
            field->fieldType = STRING_F;
            break;
    }

    free(format);

    *ending_index = pos + strlen("{'':'','':''}") + strlen(JSON_FIELD_NAME) + strlen(JSON_FIELD_TYPE) + strlen(field->field_name) + 1;

    regfree(&regexp_rec);

    return field;
}

void destroyField(Field *field)
{
    if (!field) return;
    field->fieldType = INTEGER_F;
    free(field->field_name);
    free(field);
}


TableSchema *createTableSchema(Field **fields, size_t number_of_fields, size_t key_column_index)
{
    TableSchema *tableSchema = (TableSchema*) malloc(sizeof(TableSchema));

    tableSchema->number_of_fields = number_of_fields;
    tableSchema->key_column_index = key_column_index;
    tableSchema->fields = fields;

    return tableSchema;
}

char *transformTableSchemaToJSON(TableSchema *tableSchema)
{
    /// {"KEY_COL_I":"","":"NUM_OF_FIELDS":"","FIELDS":[]}

    if (!tableSchema) return "";

    size_t field_lengths = 0, offset = 0;

    for (size_t i = 0; i < tableSchema->number_of_fields; i++) {
        char *field_line = transformFieldToJSON(tableSchema->fields[i]);
        field_lengths += strlen(field_line);
        free(field_line);
    }

    char *fields = (char*) malloc(field_lengths + tableSchema->number_of_fields);

    for (size_t i = 0; i < tableSchema->number_of_fields; i++) {
        char *field = transformFieldToJSON(tableSchema->fields[i]);
        memcpy(fields + offset, field, strlen(field));
        offset += (strlen(field) + 1);
        fields[offset - 1] = ',';
        free(field);
    }

    fields[field_lengths + tableSchema->number_of_fields - 1] = '\0';

    char *line = (char*) malloc(strlen("{'':'','':'','':[]}") + strlen(JSON_SCHEMA_KEY_I) + strlen(JSON_SCHEMA_NUM_OF_FIELDS) + strlen(JSON_SCHEMA_FIELDS) +
            field_lengths + tableSchema->number_of_fields + 64);

    sprintf(line, "{\"%s\":\"%zd\",\"%s\":\"%zd\",\"%s\":[%s]}", JSON_SCHEMA_KEY_I, tableSchema->key_column_index, JSON_SCHEMA_NUM_OF_FIELDS, tableSchema->number_of_fields,
            JSON_SCHEMA_FIELDS, fields);

    free(fields);

    return line;
}

TableSchema *parseTableSchemaJSON(const char *line, size_t pos, size_t *ending_index)
{
    if (!line || pos >= strlen(line)) return NULL;

    char *long_substr_reg = "(.+?)";
    char *format = (char*) malloc(strlen("^\\{'':'','':'','':\\[\\]\\}$") + strlen(JSON_SCHEMA_KEY_I) + strlen(JSON_SCHEMA_NUM_OF_FIELDS) +
            strlen(JSON_SCHEMA_FIELDS) + strlen(long_substr_reg) * 3 + 1);
    sprintf(format, "^\\{\"%s\":\"%s\",\"%s\":\"%s\",\"%s\":\\[%s\\]\\}$", JSON_SCHEMA_KEY_I, long_substr_reg, JSON_SCHEMA_NUM_OF_FIELDS, long_substr_reg,
            JSON_SCHEMA_FIELDS, long_substr_reg);

    regex_t regexp_rec;
    regmatch_t groups[4];
    int comp_ec, exec_ec;

    comp_ec = regcomp(&regexp_rec, format, REG_EXTENDED);

    if (comp_ec != REG_NOERROR) return NULL;

    exec_ec = regexec(&regexp_rec, line + pos, 4, groups, 0);

    if (exec_ec == REG_NOMATCH) return NULL;

    char *string_key_col_i = substrToNewInstance(line, groups[1].rm_so, groups[1].rm_eo);
    char *string_num_of_fields = substrToNewInstance(line, groups[2].rm_so, groups[2].rm_eo);
    char *string_fields = substrToNewInstance(line, groups[3].rm_so, groups[3].rm_eo);

    size_t key_col_i = string_to_size_t(string_key_col_i);
    size_t num_of_fields = string_to_size_t(string_num_of_fields);

    if (num_of_fields == 0 || key_col_i >= num_of_fields) return NULL;

    free(format);

    regfree(&regexp_rec);

    Field **fields = (Field**) malloc(sizeof(Field*) * num_of_fields);
    char *field_reg = "(\\{.+?\\}),";
    size_t offset = 1;

    char *fields_format = (char*) malloc(strlen(field_reg) * num_of_fields + 2);
    fields_format[0] = '^';
    for (size_t i = 0; i < num_of_fields; i++, offset += strlen(field_reg)) memcpy(fields_format + offset, field_reg, strlen(field_reg) + 1);
    fields_format[offset - 1] = '$';
    fields_format[offset] = '\0';

    regex_t regexp_fields_rec;
    regmatch_t fields_groups[num_of_fields + 1];
    int comp_fields_ec, exec_fields_ec;

    comp_fields_ec = regcomp(&regexp_fields_rec, fields_format, REG_EXTENDED);

    if (comp_fields_ec != REG_NOERROR) return NULL;

    exec_fields_ec = regexec(&regexp_fields_rec, string_fields, num_of_fields + 1, fields_groups, 0);

    if (exec_fields_ec == REG_NOMATCH) return NULL;

    char *s_fields[num_of_fields];
    size_t f_ending_index = 0;
    *ending_index = 0;

    for (size_t i = 1; i < num_of_fields + 1; i++) s_fields[i - 1] = substrToNewInstance(string_fields, fields_groups[i].rm_so, fields_groups[i].rm_eo);

    for (size_t i = 0; i < num_of_fields; i++) {
        fields[i] = parseFieldJSON(s_fields[i], 0, &f_ending_index);
        *ending_index += (f_ending_index + 1);
        free(s_fields[i]);
    }

    (*ending_index) += (strlen("{'':'','':'','':[]}") + strlen(JSON_SCHEMA_KEY_I) + strlen(JSON_SCHEMA_NUM_OF_FIELDS) + strlen(JSON_SCHEMA_FIELDS));
    (*ending_index) += (strlen(string_key_col_i) + strlen(string_num_of_fields));
    (*ending_index)--;

    TableSchema *tableSchema = createTableSchema(fields, num_of_fields, key_col_i);

    free(string_fields);
    free(fields_format);
    free(string_key_col_i);
    free(string_num_of_fields);

    regfree(&regexp_fields_rec);

    return tableSchema;
}

void destroyTableSchema(TableSchema *tableSchema)
{
    if (!tableSchema || !tableSchema->fields) return;
    for (size_t i = 0; i < tableSchema->number_of_fields; i++)
        if (tableSchema->fields[i]) destroyField(tableSchema->fields[i]);

    free(tableSchema->fields);

    tableSchema->fields = NULL;
    tableSchema->number_of_fields = 0;
    tableSchema->key_column_index = 0;

    free(tableSchema);
}


char *substrToNewInstance(const char *origin, size_t begin, size_t end)
{
    if (!origin || begin >= strlen(origin) || end > strlen(origin) || begin >= end) return NULL;

    char *cell = (char*) malloc(end - begin + 1);
    memcpy(cell, origin + begin, end - begin);
    cell[end - begin] = '\0';

    return cell;
}

char *createDataCell(const char *value)
{
    if (!value) return NULL;

    char *cell = (char*) malloc(strlen(value) + 1);
    memcpy(cell, value, strlen(value) + 1);
    cell[strlen(value)] = '\0';

    return cell;
}

TableRecord *createTableRecord(size_t length, char **dataCells)
{
    TableRecord *tableRecord = (TableRecord*) malloc(sizeof(TableRecord));

    tableRecord->length = length;
    tableRecord->next_record = NULL;
    tableRecord->prev_record = NULL;
    tableRecord->dataCells = dataCells;

    return tableRecord;
}

char *transformTableRecordToJSON(TableRecord *tableRecord)
{
    /// {"RECORD":[]}

    if (!tableRecord) return "";

    size_t cell_values_length = 0, offset = 0;
    for (size_t i = 0; i < tableRecord->length; i++) cell_values_length += strlen(tableRecord->dataCells[i]);

    char *record_data = (char*) malloc(tableRecord->length * 3 + cell_values_length);

    for (size_t i = 0; i < tableRecord->length; i++) {
        record_data[offset++] = '"';
        memcpy(record_data + offset, tableRecord->dataCells[i], strlen(tableRecord->dataCells[i]) + 1);
        offset += (strlen(tableRecord->dataCells[i]) + 1);
        record_data[offset - 1] = '"';
        record_data[offset++] = ',';
    }

    record_data[tableRecord->length * 3 + cell_values_length - 1] = '\0';

    char *line = (char*) malloc(strlen("{'':[]}") + strlen(JSON_RECORD) + tableRecord->length * 3 + cell_values_length);
    sprintf(line, "{\"%s\":[%s]}", JSON_RECORD, record_data);

    free(record_data);

    return line;
}

TableRecord *parseTableRecordJSON(const char *line, size_t pos, size_t *ending_index, TableSchema *tableSchema)
{
    if (!line || !tableSchema || tableSchema->number_of_fields == 0 || pos >= strlen(line)) return NULL;

    char *arg_regexp = "\"(.+?)\",", *end_regexp = "\\]\\}$";
    char *format = (char*) malloc(strlen("{'':[]}") + strlen(JSON_RECORD) + tableSchema->number_of_fields * strlen(arg_regexp) + 6);
    sprintf(format, "^\\{\"%s\":\\[", JSON_RECORD);

    size_t args_offset = strlen(format);
    for (size_t i = 0; i < tableSchema->number_of_fields; i++) memcpy(format + args_offset + i * strlen(arg_regexp), arg_regexp, strlen(arg_regexp) + 1);
    memcpy(format + strlen(format) - 1, end_regexp, strlen(end_regexp) + 1);

    regex_t regexp_rec;
    regmatch_t groups[tableSchema->number_of_fields + 1];
    int comp_ec, exec_ec;

    comp_ec = regcomp(&regexp_rec, format, REG_EXTENDED);

    if (comp_ec != REG_NOERROR) return NULL;

    exec_ec = regexec(&regexp_rec, line + pos, tableSchema->number_of_fields + 1, groups, 0);

    if (exec_ec == REG_NOMATCH) return NULL;

    char **cells = (char**) malloc(sizeof(char*) * tableSchema->number_of_fields);
    for (size_t i = 1; i < tableSchema->number_of_fields + 1; i++) cells[i - 1] = substrToNewInstance(line,groups[i].rm_so,groups[i].rm_eo);

    TableRecord *tableRecord = createTableRecord(tableSchema->number_of_fields, cells);

    free(format);

    *ending_index = pos + strlen("{'':[]}") + strlen(JSON_RECORD);
    for (size_t i = 0; i < tableSchema->number_of_fields; i++) *ending_index += (strlen("'',") + strlen(cells[i]));
    (*ending_index)--;

    regfree(&regexp_rec);

    return tableRecord;
}

void destroyTableRecord(TableRecord *tableRecord)
{
    if (!tableRecord || !tableRecord->dataCells) return;
    for (size_t i = 0; i < tableRecord->length; i++)
        if (tableRecord->dataCells[i]) free(tableRecord->dataCells[i]);

    free(tableRecord->dataCells);

    tableRecord->dataCells = NULL;

    if (!tableRecord->prev_record) {
        if (tableRecord->next_record) tableRecord->next_record->prev_record = NULL;
    } else {
        if (!tableRecord->next_record) tableRecord->prev_record->next_record = NULL;
        else {
            tableRecord->prev_record->next_record = tableRecord->next_record;
            tableRecord->next_record->prev_record = tableRecord->prev_record;
        }
    }

    free(tableRecord);
}


Table *createTable(TableSchema *tableSchema, char *table_name)
{
    Table *table = (Table*) malloc(sizeof(Table));

    table->tableSchema = tableSchema;
    table->table_name = table_name;
    table->length = 0;
    table->firstTableRecord = NULL;

    return table;
}

void insertTableRecord(Table *table, TableRecord *tableRecord)
{
    if (!table || !tableRecord) return;

    table->length++;

    if (!table->firstTableRecord) table->firstTableRecord = tableRecord;
    else {
        tableRecord->next_record = table->firstTableRecord;
        table->firstTableRecord->prev_record = tableRecord;
        table->firstTableRecord = tableRecord;
    }
}

void destroyTable(Table *table)
{
    if (!table) return;
    table->table_name = "";

    TableRecord **tableRecords = (TableRecord**) malloc(sizeof(TableRecord*) * table->length);
    TableRecord *current = table->firstTableRecord;

    if (current != NULL) {
        for (size_t i = 0; i < table->length; i++, current = current->next_record) tableRecords[i] = current;
        for (size_t i = 0; i < table->length; i++) destroyTableRecord(tableRecords[i]);
    }

    free(tableRecords);

    table->length = 0;
    table->firstTableRecord = NULL;
    free(table);
}


TableLink *createTableLink(char *table_name, size_t link)
{
    if (!table_name) return NULL;
    TableLink *tableLink = (TableLink*) malloc(sizeof(TableLink));
    tableLink->table_name = table_name;
    tableLink->link = link;
    return tableLink;
}

char *transformTableLinkToJSON(TableLink *tableLink)
{
    /// {"T_NAME":"","T_LINK":""}

    if (!tableLink) return "";
    char *line = (char*) malloc(strlen("{'':'','':''}") + strlen(JSON_TABLE_NAME) + strlen(JSON_TABLE_LINK) + strlen(tableLink->table_name) + 49);
    sprintf(line, "{\"%s\":\"%s\",\"%s\":\"%zd\"}", JSON_TABLE_NAME, tableLink->table_name, JSON_TABLE_LINK, tableLink->link);

    return line;
}

TableLink *parseTableLinkJSON(const char *line, size_t pos, size_t *ending_index)
{
    if (!line || pos >= strlen(line)) return NULL;

    char *substring_reg = "(.+?)";
    char *format = (char*) malloc(strlen("^\\{'':'(.+?)','':'(.+?)'\\}$") + strlen(JSON_TABLE_NAME) + strlen(JSON_TABLE_LINK) + 1);
    sprintf(format, "^\\{\"%s\":\"%s\",\"%s\":\"%s\"\\}$", JSON_TABLE_NAME, substring_reg, JSON_TABLE_LINK, substring_reg);

    regex_t regexp_rec;
    regmatch_t groups[3];
    int comp_ec, exec_ec;

    comp_ec = regcomp(&regexp_rec, format, REG_EXTENDED);

    if (comp_ec != REG_NOERROR) return NULL;

    exec_ec = regexec(&regexp_rec, line + pos, 3, groups, 0);

    if (exec_ec == REG_NOMATCH) return NULL;

    TableLink *tableLink = createTableLink(substrToNewInstance(line, groups[1].rm_so, groups[1].rm_eo), 0);

    char *string_link = substrToNewInstance(line, groups[2].rm_so, groups[2].rm_eo);
    size_t link = 0;
    for (size_t i = 0; i < strlen(string_link); i++, link *= 10) link += (string_link[i] - '0');
    link /= 10;
    free(format);

    tableLink->link = link;

    *ending_index = pos + strlen("{'':'','':''}") + strlen(JSON_TABLE_NAME) + strlen(JSON_TABLE_LINK) + strlen(tableLink->table_name) + strlen(string_link);

    free(string_link);

    regfree(&regexp_rec);

    return tableLink;
}

void destroyTableLink(TableLink *tableLink, int save_name)
{
    if (!tableLink) return;
    tableLink->link = 0;
    if (!save_name) free(tableLink->table_name);
    free(tableLink);
}