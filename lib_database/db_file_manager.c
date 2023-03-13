//
// Created by yars on 17.10.22.
//

#include "db_file_manager.h"

size_t min_size(size_t a, size_t b)
{
    return a < b ? a : b;
}

void freePage(DataPage *dataPage)
{
    if (!dataPage) return;
    dataPage->header.flags = 0;
    dataPage->header.next_related_page = dataPage->header.page_index;
    dataPage->header.data_size = 0;
    updatePageStatus(dataPage, PAGE_STATUS_INACTIVE);
    memset(dataPage->header.metadata,'\0',PAGE_METADATA_SIZE);
    memset(dataPage->page_data,'\0',PAGE_DATA_SIZE);
}

void freePageThread(const char *filename, size_t page_index)
{
    if (!filename) return;
    for (uint64_t current = page_index, last = current + 1; current != last;) {
        last = current;
        DataPage *dataPage = (DataPage*) malloc(sizeof(DataPage));
        readDataPage(filename, dataPage, current);
        current = dataPage->header.next_related_page;
        freePage(dataPage);
        writeDataPage(filename, dataPage);
        free(dataPage);
    }
}

void freeDatabaseFile(const char *filename)
{
    FILE *file = fopen(filename, "w");
    fclose(file);
}

void createDatabasePage(const char *filename, const char *db_name)
{
    if (strlen(db_name) >= PAGE_E_HEADER_MD_SIZE) return;
    DataPage *dbHeaderPage = (DataPage*) malloc(sizeof(DataPage));
    freeDatabaseFile(filename);
    freePage(dbHeaderPage);
    updatePageType(dbHeaderPage, PAGE_TYPE_DATABASE_HEADER);
    updatePageStatus(dbHeaderPage, PAGE_STATUS_ACTIVE);
    dbHeaderPage->header.page_index = 0;
    dbHeaderPage->header.next_related_page = dbHeaderPage->header.page_index;
    dbHeaderPage->header.data_size = 0;
    updateNumberOfPages(dbHeaderPage, 1);
    updateNumberOfTables(dbHeaderPage, 0);
    updateHeaderPageMetadata(dbHeaderPage, db_name);
    writeDataPage(filename, dbHeaderPage);
    free(dbHeaderPage);
}

void expandDBFile(const char *filename)
{
    if (!filename) return;

    DataPage *dbHeaderPage = (DataPage*) malloc(sizeof(DataPage));
    readDataPage(filename, dbHeaderPage, PAGE_DB_ROOT_INDEX);
    int32_t index = getNumberOfPages(dbHeaderPage);
    if (index != PAGE_CORRUPT_EXITCODE) updateNumberOfPages(dbHeaderPage, index + 1);
    writeDataPage(filename, dbHeaderPage);
    free(dbHeaderPage);

    if (index != PAGE_CORRUPT_EXITCODE) {
        DataPage *newPage = (DataPage*) malloc(sizeof(DataPage));
        freePage(newPage);
        newPage->header.page_index = index;
        newPage->header.next_related_page = newPage->header.page_index;
        writeDataPage(filename, newPage);
        free(newPage);
    }
}

size_t expandPageThread(const char *filename, size_t page_index)
{
    if (!filename) return PAGE_SEARCH_FAILED;

    uint64_t current = page_index;
    for (uint64_t last = current + 1; current != last;) {
        last = current;
        DataPage *dataPage = (DataPage*) malloc(sizeof(DataPage));
        readDataPage(filename, dataPage, current);
        current = dataPage->header.next_related_page;
        free(dataPage);
    }

    size_t thread_tail_index = current, new_page_index = findFreePageOrExpand(filename, PAGE_DB_ROOT_INDEX);
    u_int8_t flags;

    if (new_page_index == PAGE_SEARCH_FAILED) return PAGE_SEARCH_FAILED;

    DataPage *dataPage = (DataPage*) malloc(sizeof(DataPage));
    readDataPage(filename, dataPage, thread_tail_index);
    dataPage->header.page_index = thread_tail_index;
    dataPage->header.next_related_page = new_page_index;
    flags = dataPage->header.flags;
    writeDataPage(filename, dataPage);
    free(dataPage);

    DataPage *newDataPage = (DataPage*) malloc(sizeof(DataPage));
    freePage(newDataPage);
    newDataPage->header.flags = flags;
    newDataPage->header.page_index = new_page_index;
    newDataPage->header.next_related_page = newDataPage->header.page_index;
    newDataPage->header.data_size = 0;
    writeDataPage(filename, newDataPage);
    free(newDataPage);

    return new_page_index;
}

size_t findFreePageOrExpand(const char *filename, size_t starting_point)
{
    if (!filename) return PAGE_SEARCH_FAILED;

    DataPage *dbHeaderPage = (DataPage*) malloc(sizeof(DataPage));
    readDataPage(filename, dbHeaderPage, PAGE_DB_ROOT_INDEX);
    size_t max_index = getNumberOfPages(dbHeaderPage), found = 0;
    free(dbHeaderPage);

    for (size_t current = starting_point; current < max_index && !found; current++) {
        DataPage *dataPage = (DataPage *) malloc(sizeof(DataPage));
        readDataPage(filename, dataPage, current);
        if (getPageStatus(dataPage) == PAGE_STATUS_INACTIVE) found = current;
        free(dataPage);
    }

    if (found) return found;

    expandDBFile(filename);
    return max_index;
}

void readDataPage(const char *filename, DataPage *dataPage, size_t page_index)
{
    if (!filename || !dataPage) return;

    FILE *file = fopen(filename, "rb+");

    if (!file) return;

    fseek(file, PAGE_SIZE * page_index, SEEK_SET);
    fread(dataPage, sizeof(DataPage), 1, file);
    fclose(file);
}

void writeDataPage(const char *filename, DataPage *dataPage)
{
    if (!filename || !dataPage) return;

    FILE *file = fopen(filename, "rb+");

    if (!file) return;

    fseek(file, PAGE_SIZE * dataPage->header.page_index, SEEK_SET);
    fwrite(dataPage, sizeof(DataPage), 1, file);
    fclose(file);
}

void updatePageMetadata(DataPage *dataPage, const char *metadata)
{
    if (!dataPage || !metadata) return;
    memset(dataPage->header.metadata,'\0',PAGE_METADATA_SIZE);
    memcpy(dataPage->header.metadata, metadata, min_size(strlen(metadata), PAGE_METADATA_SIZE - 1));
}

void updatePageData(DataPage *dataPage, const char *data)
{
    if (!dataPage || !data) return;
    memset(dataPage->page_data,'\0',PAGE_DATA_SIZE);
    memcpy(dataPage->page_data, data, min_size(strlen(data), PAGE_DATA_SIZE - 1));
    dataPage->header.data_size = min_size(strlen(data) + 1, PAGE_DATA_SIZE);
}

void updateHeaderPageMetadata(DataPage *dataPage, const char *metadata)
{
    if (!dataPage || !metadata) return;
    memset(dataPage->header.metadata + sizeof(u_int32_t) * 2, '\0', PAGE_E_HEADER_MD_SIZE);
    memcpy(dataPage->header.metadata + sizeof(u_int32_t) * 2, metadata, min_size(strlen(metadata), PAGE_E_HEADER_MD_SIZE - 1));
}

void updateNumberOfPages(DataPage *dataPage, u_int32_t num)
{
    if (!dataPage) return;
    *(u_int32_t*)(dataPage->header.metadata) = num;
}

void updateNumberOfTables(DataPage *dataPage, u_int32_t num)
{
    if (!dataPage) return;
    *(u_int32_t*)(dataPage->header.metadata + sizeof(u_int32_t)) = num;
}

void updateTableLength(DataPage *dataPage, u_int32_t num)
{
    if (!dataPage) return;
    *(u_int32_t*)(dataPage->header.metadata) = num;
}

void updateTableMaxID(DataPage *dataPage, u_int32_t num)
{
    if (!dataPage) return;
    *(u_int32_t*)(dataPage->header.metadata + sizeof(u_int32_t)) = num;
}

char *getPageMetadata(DataPage *dataPage)
{
    if (!dataPage) return NULL;
    return &*(dataPage->header.metadata);
}

char *getPageData(DataPage *dataPage)
{
    if (!dataPage) return NULL;
    return &*(dataPage->page_data);
}

char *getHeaderPageMetadata(DataPage *dataPage)
{
    if (!dataPage) return NULL;
    return &*(dataPage->header.metadata + sizeof(u_int32_t) * 2);
}

int32_t getNumberOfPages(DataPage *dataPage)
{
    if (!dataPage) return PAGE_CORRUPT_EXITCODE;
    return *(int32_t*)(dataPage->header.metadata);
}

int32_t getNumberOfTables(DataPage *dataPage)
{
    if (!dataPage) return PAGE_CORRUPT_EXITCODE;
    return *(int32_t*)(dataPage->header.metadata + sizeof(u_int32_t));
}

int32_t getTableLength(DataPage *dataPage)
{
    if (!dataPage) return PAGE_CORRUPT_EXITCODE;
    return *(int32_t*)(dataPage->header.metadata);
}

int32_t getTableMaxID(DataPage *dataPage)
{
    if (!dataPage) return PAGE_CORRUPT_EXITCODE;
    return *(int32_t*)(dataPage->header.metadata + sizeof(u_int32_t));
}


int getPageType(DataPage *dataPage)
{
    if (!dataPage) return PAGE_CORRUPT_EXITCODE;
    return dataPage->header.flags & PAGE_TYPE_MASK;
}

int getPageStatus(DataPage *dataPage)
{
    if (!dataPage) return PAGE_CORRUPT_EXITCODE;
    return dataPage->header.flags & PAGE_STATUS_MASK;
}

void updatePageType(DataPage *dataPage, u_int8_t new_type)
{
    if (!dataPage) return;
    dataPage->header.flags &= ~PAGE_TYPE_MASK;
    dataPage->header.flags |= (new_type & PAGE_TYPE_MASK);
}

void updatePageStatus(DataPage *dataPage, u_int8_t new_status)
{
    if (!dataPage) return;
    dataPage->header.flags &= ~PAGE_STATUS_MASK;
    dataPage->header.flags |= (new_status & PAGE_STATUS_MASK);
}

void appendData(DataPage *dataPage, const char *line)
{
    if (!dataPage || !line) return;
    if (dataPage->header.data_size + strlen(line) + 1 > PAGE_DATA_SIZE) return;
    if (dataPage->header.data_size == 0) updatePageData(dataPage, line);
    else {
        memcpy(getPageData(dataPage) + dataPage->header.data_size, line, strlen(line) + 1);
        dataPage->header.data_size += (strlen(line) + 1);
    }
}

void appendDataOrExpandThread(const char *filename, size_t page_index, const char *line)
{
    if (!line || strlen(line) >= PAGE_DATA_SIZE) return;

    DataPage *dbHeader = (DataPage*) malloc(sizeof(DataPage));
    readDataPage(filename, dbHeader, PAGE_DB_ROOT_INDEX);
    size_t max_index = getNumberOfPages(dbHeader);
    free(dbHeader);

    ssize_t found = SEARCH_PAGE_NOT_FOUND;
    size_t current = page_index;
    for (size_t last = current + 1; current < max_index && (found == SEARCH_PAGE_NOT_FOUND) && current != last;) {
        last = current;
        DataPage *dataPage = (DataPage*) malloc(sizeof(DataPage));
        readDataPage(filename, dataPage, current);
        if (dataPage->header.data_size + strlen(line) + 1 <= PAGE_DATA_SIZE) found = (ssize_t) current;
        current = dataPage->header.next_related_page;
        free(dataPage);
    }

    if (found != SEARCH_PAGE_NOT_FOUND) {
        DataPage *dataPage = (DataPage*) malloc(sizeof(DataPage));
        readDataPage(filename, dataPage, found);
        appendData(dataPage, line);
        writeDataPage(filename, dataPage);
        free(dataPage);
    } else {
        size_t new_page = findFreePageOrExpand(filename, PAGE_DB_ROOT_INDEX);
        int flags;
        if (new_page == PAGE_SEARCH_FAILED) return;

        DataPage *tailPage = (DataPage*) malloc(sizeof(DataPage));
        readDataPage(filename, tailPage, current);
        tailPage->header.next_related_page = new_page;
        flags = tailPage->header.flags;
        writeDataPage(filename, tailPage);
        free(tailPage);

        DataPage *newPage = (DataPage*) malloc(sizeof(DataPage));
        readDataPage(filename, newPage, new_page);
        newPage->header.flags = flags;
        newPage->header.page_index = new_page;
        newPage->header.next_related_page = newPage->header.page_index;
        appendData(newPage, line);
        writeDataPage(filename, newPage);
        free(newPage);
    }
}

size_t findTableOnPage(DataPage *dataPage, const char *table_name, size_t *checked) {
    if (!dataPage || !table_name) return PAGE_SEARCH_FAILED;

    size_t index = 0, parsed = 0, found = 0;
    while (dataPage->page_data[index] == '\0' && index < PAGE_DATA_SIZE) index++;

    while (!found && index < PAGE_DATA_SIZE) {
        TableLink *tableLink = parseTableLinkJSON(dataPage->page_data + index, 0, &parsed);
        index += parsed;
        if (tableLink != NULL && strcmp(table_name, tableLink->table_name) == 0) found = tableLink->link;
        destroyTableLink(tableLink, 0);
        while (dataPage->page_data[index] == '\0' && index < PAGE_DATA_SIZE) index++;
        (*checked)++;
    }

    if (!found) return PAGE_SEARCH_FAILED;

    return found;
}

size_t findTable(const char *filename, const char *table_name)
{
    if (!filename || !table_name) return PAGE_SEARCH_FAILED;

    DataPage *dbHeader = (DataPage*) malloc(sizeof(DataPage));
    readDataPage(filename, dbHeader, PAGE_DB_ROOT_INDEX);
    int32_t num_of_tables = getNumberOfTables(dbHeader);
    int32_t num_of_pages = getNumberOfPages(dbHeader);
    free(dbHeader);

    if (num_of_tables == PAGE_CORRUPT_EXITCODE || num_of_pages == PAGE_CORRUPT_EXITCODE) return PAGE_SEARCH_FAILED;

    size_t found = 0, iterated = 0;
    for (uint64_t current = PAGE_DB_ROOT_INDEX, last = current + 1; current != last && iterated < num_of_tables && !found;) {
        last = current;
        DataPage *dataPage = (DataPage*) malloc(sizeof(DataPage));
        readDataPage(filename, dataPage, current);
        current = dataPage->header.next_related_page;
        found = findTableOnPage(dataPage, table_name, &iterated);
        free(dataPage);
    }

    if (!found) return PAGE_SEARCH_FAILED;

    return found;
}

TableSchema *getSchema(const char *filename, const char *table_name)
{
    if (!filename || !table_name) return NULL;
    size_t index = findTable(filename, table_name), pos = 0;
    if (index == PAGE_SEARCH_FAILED) return NULL;

    DataPage *tableHeader = (DataPage*) malloc(sizeof(DataPage));
    readDataPage(filename, tableHeader, index);
    TableSchema *tableSchema = parseTableSchemaJSON(tableHeader->page_data, 0, &pos);
    free(tableHeader);

    return tableSchema;
}

int addTableHeader(const char *filename, Table *table)
{
    if (!filename || !table || !table->tableSchema) return -1;
    size_t search_result = findTable(filename, table->table_name);
    if (search_result != SEARCH_TABLE_NOT_FOUND) return -1;

    size_t table_header = findFreePageOrExpand(filename, PAGE_DB_ROOT_INDEX);
    if (table_header == PAGE_SEARCH_FAILED) return -1;

    DataPage *dbHeader = (DataPage*) malloc(sizeof(DataPage));
    readDataPage(filename, dbHeader, PAGE_DB_ROOT_INDEX);
    updateNumberOfTables(dbHeader, getNumberOfTables(dbHeader) + 1);
    writeDataPage(filename, dbHeader);
    free(dbHeader);

    TableLink *tableLink = createTableLink(table->table_name, table_header);
    char *table_link = transformTableLinkToJSON(tableLink);
    destroyTableLink(tableLink, 1);
    appendDataOrExpandThread(filename, PAGE_DB_ROOT_INDEX, table_link);
    free(table_link);

    DataPage *tableHeader = (DataPage*) malloc(sizeof(DataPage));
    readDataPage(filename, tableHeader, table_header);
    freePage(tableHeader);
    char *table_schema = transformTableSchemaToJSON(table->tableSchema);
    updatePageStatus(tableHeader, PAGE_STATUS_ACTIVE);
    updatePageType(tableHeader, PAGE_TYPE_TABLE_HEADER);
    updateTableLength(tableHeader, table->length);
    updateHeaderPageMetadata(tableHeader, table->table_name);
    updatePageData(tableHeader, table_schema);
    tableHeader->header.data_size = PAGE_DATA_SIZE;
    writeDataPage(filename, tableHeader);
    free(table_schema);
    free(tableHeader);

    size_t table_data = findFreePageOrExpand(filename, PAGE_DB_ROOT_INDEX);
    if (table_data == PAGE_SEARCH_FAILED) return -1;

    DataPage *tableData = (DataPage*) malloc(sizeof(DataPage));
    readDataPage(filename, tableData, table_header);
    freePage(tableData);
    updatePageStatus(tableData, PAGE_STATUS_ACTIVE);
    updatePageType(tableData, PAGE_TYPE_TABLE_DATA);
    tableData->header.page_index = table_data;
    tableData->header.next_related_page = tableData->header.page_index;
    writeDataPage(filename, tableData);
    free(tableData);

    for (TableRecord *tableRecord = table->firstTableRecord; tableRecord != NULL; tableRecord = tableRecord->next_record) {
        char *table_record = transformTableRecordToJSON(tableRecord);
        appendDataOrExpandThread(filename, table_data, table_record);
        free(table_record);
    }

    tableHeader = (DataPage*) malloc(sizeof(DataPage));
    readDataPage(filename, tableHeader, table_header);
    tableHeader->header.next_related_page = table_data;
    writeDataPage(filename, tableHeader);
    free(tableHeader);

    return 0;
}

int insertTableRecords(const char *filename, Table *table)
{
    if (!filename || !table || !table->tableSchema || table->length == 0) return -1;
    size_t search_result = findTable(filename, table->table_name);
    if (search_result == SEARCH_TABLE_NOT_FOUND) return -1;

    DataPage *tableHeader = (DataPage*) malloc(sizeof(DataPage));
    readDataPage(filename, tableHeader, search_result);
    updateTableLength(tableHeader, getTableLength(tableHeader) + table->length);
    writeDataPage(filename, tableHeader);
    free(tableHeader);

    for (TableRecord *tableRecord = table->firstTableRecord; tableRecord != NULL; tableRecord = tableRecord->next_record) {
        char *table_record = transformTableRecordToJSON(tableRecord);
        appendDataOrExpandThread(filename, search_result, table_record);
        free(table_record);
    }

    return 0;
}

size_t findAndErase(DataPage *dataPage, const char *table_name, size_t *checked)
{
    if (!dataPage || !table_name) return PAGE_SEARCH_FAILED;

    size_t index = 0, parsed = 0, found = 0, stop = 0;
    while (dataPage->page_data[index] == '\0' && index < PAGE_DATA_SIZE) index++;

    while (!stop && index < PAGE_DATA_SIZE) {
        TableLink *tableLink = parseTableLinkJSON(dataPage->page_data + index, 0, &parsed);
        if (tableLink != NULL && strcmp(table_name, tableLink->table_name) == 0) {
            found = index;
            stop = 1;
        }
        index += parsed;
        destroyTableLink(tableLink, 0);
        while (dataPage->page_data[index] == '\0' && index < PAGE_DATA_SIZE) index++;
        (*checked)++;
    }

    memset(dataPage->page_data + found, '\0', parsed);

    if (!found) return PAGE_SEARCH_FAILED;

    return found;
}

void findAndUpdateMaxID(const char *filename, const char *table_name, uint32_t new_max_id)
{
    if (!filename || !table_name) return;
    size_t search_result = findTable(filename, table_name);
    if (search_result == SEARCH_TABLE_NOT_FOUND) return;
    DataPage *tableHeader = (DataPage*) malloc(sizeof(DataPage));
    readDataPage(filename, tableHeader, search_result);
    updateTableMaxID(tableHeader, new_max_id);
    writeDataPage(filename, tableHeader);
    free(tableHeader);
}

uint32_t findAndGetMaxID(const char *filename, const char *table_name)
{
    if (!filename || !table_name) return 0;
    size_t search_result = findTable(filename, table_name);
    if (search_result == SEARCH_TABLE_NOT_FOUND) return 0;
    DataPage *tableHeader = (DataPage*) malloc(sizeof(DataPage));
    readDataPage(filename, tableHeader, search_result);
    uint32_t maxID = getTableMaxID(tableHeader);
    free(tableHeader);
    return maxID;
}

int deleteTable(const char *filename, const char *table_name)
{
    if (!filename || !table_name) return -1;
    size_t search_result = findTable(filename, table_name);
    if (search_result == SEARCH_TABLE_NOT_FOUND) return -1;
    freePageThread(filename, search_result);

    DataPage *dbHeader = (DataPage*) malloc(sizeof(DataPage));
    readDataPage(filename, dbHeader, PAGE_DB_ROOT_INDEX);
    int32_t num_of_tables = getNumberOfTables(dbHeader);
    int32_t num_of_pages = getNumberOfPages(dbHeader);
    updateNumberOfTables(dbHeader, num_of_tables - 1);
    writeDataPage(filename, dbHeader);
    free(dbHeader);

    if (num_of_tables == PAGE_CORRUPT_EXITCODE || num_of_pages == PAGE_CORRUPT_EXITCODE) return -1;

    size_t found = 0, iterated = 0;
    for (uint64_t current = PAGE_DB_ROOT_INDEX, last = current + 1; current != last && iterated < num_of_tables && !found;) {
        last = current;
        DataPage *dataPage = (DataPage*) malloc(sizeof(DataPage));
        readDataPage(filename, dataPage, current);
        current = dataPage->header.next_related_page;
        found = findAndErase(dataPage, table_name, &iterated);
        writeDataPage(filename, dataPage);
        free(dataPage);
    }
    return 0;
}

void printTable(const char *filename, const char *table_name, size_t num_of_filters, SearchFilter **filters)
{
    if (!filename || !table_name) return;
    size_t search_result = findTable(filename, table_name);
    if (search_result == SEARCH_TABLE_NOT_FOUND) return;

    DataPage *tableHeader = (DataPage*) malloc(sizeof(DataPage));
    readDataPage(filename, tableHeader, search_result);
    size_t length = getTableLength(tableHeader), data = tableHeader->header.next_related_page, pos;
    printf("Table name: %s\nTable length: %zd\n", getHeaderPageMetadata(tableHeader), length);
    printf("--------------------------------------------------------------------------\n");
    TableSchema *tableSchema = parseTableSchemaJSON(tableHeader->page_data, 0, &pos);

    printf("#  |  ");
    for (size_t i = 0; i < tableSchema->number_of_fields; i++) {
        char *field_type;
        switch (tableSchema->fields[i]->fieldType) {
            case INTEGER_F:
                field_type = "INTEGER";
                break;
            case FLOAT_F:
                field_type = "FLOAT";
                break;
            case BOOLEAN_F:
                field_type = "BOOLEAN";
                break;
            default:
                field_type = "STRING";
                break;
        }

        printf("%s : %s  |  ", tableSchema->fields[i]->field_name, field_type);
    }

    free(tableHeader);
    printf("\n--------------------------------------------------------------------------\n");

    size_t index = 0, page_index;
    for (uint64_t current = data, last = search_result; current != last && index < length;) {
        last = current;
        DataPage *dataPage = (DataPage*) malloc(sizeof(DataPage));
        readDataPage(filename, dataPage, current);
        current = dataPage->header.next_related_page;
        page_index = 0;

        for (; page_index < dataPage->header.data_size && index < length; page_index += (strlen(dataPage->page_data + page_index) + 1), index++) {
            while (dataPage->page_data[page_index] == '\0' && page_index < PAGE_DATA_SIZE) page_index++;
            if (page_index >= PAGE_DATA_SIZE) {
                if (index > 0) index--;
                break;
            }
            TableRecord *tableRecord = parseTableRecordJSON(dataPage->page_data + page_index, 0, &pos, tableSchema);

            switch (applyAll(tableRecord, num_of_filters, filters)) {
                case FILTER_ACCEPT:
                    printf("%zd: | ", index);
                    for (size_t i = 0; i < tableRecord->length; i++)
                        printf("%s | ", tableRecord->dataCells[i]);
                    printf("\n");
                    break;
                case FILTER_REJECT:
                    break;
                case FILTER_INCOMPATIBLE:
                    printf("%zd: Filter type error\n", index);
                    break;
            }

            destroyTableRecord(tableRecord);
        }
        free(dataPage);
    }

    destroyTableSchema(tableSchema);
}

void updateRows(const char *filename, char *table_name, TableRecord *new_value, size_t num_of_filters, SearchFilter **filters)
{
    if (!filename || !table_name) return;
    size_t search_result = findTable(filename, table_name);
    if (search_result == SEARCH_TABLE_NOT_FOUND) return;

    DataPage *tableHeader = (DataPage*) malloc(sizeof(DataPage));
    readDataPage(filename, tableHeader, search_result);
    size_t length = getTableLength(tableHeader), data = tableHeader->header.next_related_page, pos;
    TableSchema *tableSchema = parseTableSchemaJSON(tableHeader->page_data, 0, &pos);
    free(tableHeader);

    size_t index = 0, page_index, updated = 0, parent_index, expel_candidate;
    for (uint64_t current = data, last = search_result; current != last && index < length;) {
        parent_index = last;
        expel_candidate = current;
        last = current;
        DataPage *dataPage = (DataPage*) malloc(sizeof(DataPage));
        Table *updated_records = createTable(tableSchema, table_name);
        readDataPage(filename, dataPage, current);
        current = dataPage->header.next_related_page;
        page_index = 0;

        for (; page_index < dataPage->header.data_size && index < length; page_index += (strlen(dataPage->page_data + page_index) + 1), index++) {
            while (dataPage->page_data[page_index] == '\0' && page_index < PAGE_DATA_SIZE) page_index++;
            if (page_index >= PAGE_DATA_SIZE) {
                if (index > 0) index--;
                break;
            }
            TableRecord *tableRecord = parseTableRecordJSON(dataPage->page_data + page_index, 0, &pos, tableSchema);

            if (applyAll(tableRecord, num_of_filters, filters) == FILTER_ACCEPT) {
                char **new_cells = (char**) malloc(sizeof(char*) * tableRecord->length);
                for (size_t i = 0; i < tableRecord->length; i++)
                    if (i == tableSchema->key_column_index) new_cells[i] = createDataCell(tableRecord->dataCells[i]);
                    else new_cells[i] = createDataCell(new_value->dataCells[i]);

                TableRecord *newRec = createTableRecord(tableRecord->length, new_cells);
                insertTableRecord(updated_records, newRec);

                memset(dataPage->page_data + page_index, '\0', pos);
                updated++;
            }

            destroyTableRecord(tableRecord);
        }
        writeDataPage(filename, dataPage);
        free(dataPage);

        insertTableRecords(filename, updated_records);

        for (TableRecord *curRec = updated_records->firstTableRecord; curRec != NULL; curRec = curRec->next_record)
            destroyTableRecord(curRec);

        updated_records->length = 0;
        free(updated_records);

        tableHeader = (DataPage*) malloc(sizeof(DataPage));
        readDataPage(filename, tableHeader, search_result);
        updateTableLength(tableHeader, length);
        writeDataPage(filename, tableHeader);
        free(tableHeader);

        if (checkEmpty(filename, expel_candidate)) expelPageFromThread(filename, parent_index, expel_candidate);
    }

    destroyTableSchema(tableSchema);

    printf("Table updated.\n");
}

int checkEmpty(const char *filename, size_t page_index)
{
    DataPage *dataPage = (DataPage*) malloc(sizeof(DataPage));
    readDataPage(filename, dataPage, page_index);
    int res = 1;
    for (size_t i = 0; i < PAGE_DATA_SIZE && res; i++) if (dataPage->page_data[i] != '\0') res = 0;
    free(dataPage);
    return res;
}

void expelPageFromThread(const char *filename, size_t parent_page, size_t page_index)
{
    DataPage *dataPage = (DataPage*) malloc(sizeof(DataPage));
    readDataPage(filename, dataPage, page_index);
    size_t next = parent_page;
    if (dataPage->header.next_related_page != page_index) next = dataPage->header.next_related_page;
    freePage(dataPage);
    writeDataPage(filename, dataPage);
    free(dataPage);

    DataPage *parentPage = (DataPage*) malloc(sizeof(DataPage));
    readDataPage(filename, parentPage, parent_page);
    parentPage->header.next_related_page = next;
    writeDataPage(filename, parentPage);
    free(parentPage);
}

void deleteRows(const char *filename, const char *table_name, size_t num_of_filters, SearchFilter **filters)
{
    if (!filename || !table_name) return;
    size_t search_result = findTable(filename, table_name);
    if (search_result == SEARCH_TABLE_NOT_FOUND) return;

    DataPage *tableHeader = (DataPage*) malloc(sizeof(DataPage));
    readDataPage(filename, tableHeader, search_result);
    size_t length = getTableLength(tableHeader), data = tableHeader->header.next_related_page, pos;
    TableSchema *tableSchema = parseTableSchemaJSON(tableHeader->page_data, 0, &pos);
    free(tableHeader);

    size_t index = 0, page_index, removed = 0, parent_index, expel_candidate;
    for (uint64_t current = data, last = search_result; current != last && index < length;) {
        parent_index = last;
        expel_candidate = current;
        last = current;
        DataPage *dataPage = (DataPage*) malloc(sizeof(DataPage));
        readDataPage(filename, dataPage, current);
        current = dataPage->header.next_related_page;
        page_index = 0;
        for (; page_index < dataPage->header.data_size && index < length; page_index += (strlen(dataPage->page_data + page_index) + 1), index++) {
            while (dataPage->page_data[page_index] == '\0' && page_index < PAGE_DATA_SIZE) page_index++;
            if (page_index >= PAGE_DATA_SIZE) {
                if (index > 0) index--;
                break;
            }
            TableRecord *tableRecord = parseTableRecordJSON(dataPage->page_data + page_index, 0, &pos, tableSchema);

            if (applyAll(tableRecord, num_of_filters, filters) == FILTER_ACCEPT) {
                memset(dataPage->page_data + page_index, '\0', pos);
                removed++;
            }

            destroyTableRecord(tableRecord);
        }
        writeDataPage(filename, dataPage);
        free(dataPage);

        if (checkEmpty(filename, expel_candidate)) expelPageFromThread(filename, parent_index, expel_candidate);
    }

    destroyTableSchema(tableSchema);

    tableHeader = (DataPage*) malloc(sizeof(DataPage));
    readDataPage(filename, tableHeader, search_result);
    updateTableLength(tableHeader, getTableLength(tableHeader) - removed);
    writeDataPage(filename, tableHeader);
    free(tableHeader);

    printf("Deleted %zd items.\n", removed);
}

void innerJoinSelect(const char *filename, const char *left_table, const char *right_table, size_t l_join_index, size_t r_join_index, size_t num_of_l_filters, SearchFilter **l_filters, size_t num_of_r_filters, SearchFilter **r_filters)
{
    if (!filename || !left_table || !right_table) return;
    size_t left_search = findTable(filename, left_table), right_search = findTable(filename, right_table);
    if (left_search == SEARCH_TABLE_NOT_FOUND || right_table == SEARCH_TABLE_NOT_FOUND) return;
    TableSchema *leftSchema = getSchema(filename, left_table), *rightSchema = getSchema(filename, right_table);
    if (l_join_index >= leftSchema->number_of_fields || r_join_index >= rightSchema->number_of_fields || leftSchema->fields[l_join_index]->fieldType != rightSchema->fields[r_join_index]->fieldType) {
        destroyTableSchema(leftSchema);
        destroyTableSchema(rightSchema);
        return;
    }

    printf("|  ");

    for (size_t i = 0; i < leftSchema->number_of_fields; i++) {
        char *field_type;
        switch (leftSchema->fields[i]->fieldType) {
            case INTEGER_F:
                field_type = "INTEGER";
                break;
            case FLOAT_F:
                field_type = "FLOAT";
                break;
            case BOOLEAN_F:
                field_type = "BOOLEAN";
                break;
            default:
                field_type = "STRING";
                break;
        }

        if (i != l_join_index) printf("%s.%s : %s  |  ", left_table, leftSchema->fields[i]->field_name, field_type);
    }

    char *j_type = leftSchema->fields[l_join_index]->fieldType == INTEGER_F ? "INTEGER" :
            leftSchema->fields[l_join_index]->fieldType == FLOAT_F ? "FLOAT" :
            leftSchema->fields[l_join_index]->fieldType == BOOLEAN_F ? "BOOLEAN" : "STRING";

    printf("<< %s.%s ~ %s.%s : %s >>  |  ", left_table, leftSchema->fields[l_join_index]->field_name, right_table, rightSchema->fields[r_join_index]->field_name, j_type);

    for (size_t i = 0; i < rightSchema->number_of_fields; i++) {
        char *field_type;
        switch (rightSchema->fields[i]->fieldType) {
            case INTEGER_F:
                field_type = "INTEGER";
                break;
            case FLOAT_F:
                field_type = "FLOAT";
                break;
            case BOOLEAN_F:
                field_type = "BOOLEAN";
                break;
            default:
                field_type = "STRING";
                break;
        }

        if (i != r_join_index) printf("%s.%s : %s  |  ", right_table, rightSchema->fields[i]->field_name, field_type);
    }

    printf("\n----------------------------------------------------------------------------------------------------------------------------------------------------\n");

    Table *rightTable = createTable(rightSchema, "__temp_filtered_right");
    DataPage *rightHeader = (DataPage*) malloc(sizeof(DataPage));
    readDataPage(filename, rightHeader, right_search);
    size_t right_length = getTableLength(rightHeader), right_data = rightHeader->header.next_related_page, r_pos = 0;
    free(rightHeader);

    size_t index = 0, page_index;
    for (uint64_t current = right_data, last = right_search; current != last && index < right_length;) {
        last = current;
        DataPage *dataPage = (DataPage*) malloc(sizeof(DataPage));
        readDataPage(filename, dataPage, current);
        current = dataPage->header.next_related_page;
        page_index = 0;

        for (; page_index < dataPage->header.data_size && index < right_length; page_index += (strlen(dataPage->page_data + page_index) + 1), index++) {
            while (dataPage->page_data[page_index] == '\0' && page_index < PAGE_DATA_SIZE) page_index++;
            if (page_index >= PAGE_DATA_SIZE) {
                if (index > 0) index--;
                break;
            }
            TableRecord *tableRecord = parseTableRecordJSON(dataPage->page_data + page_index, 0, &r_pos, rightSchema);

            switch (applyAll(tableRecord, num_of_r_filters, r_filters)) {
                case FILTER_ACCEPT:
                    insertTableRecord(rightTable, tableRecord);
                    break;
                case FILTER_REJECT:
                    break;
                case FILTER_INCOMPATIBLE:
                    printf("%zd: Filter type error\n", index);
                    break;
            }
        }
        free(dataPage);
    }

    DataPage *leftHeader = (DataPage*) malloc(sizeof(DataPage));
    readDataPage(filename, leftHeader, left_search);
    size_t left_length = getTableLength(rightHeader), left_data = rightHeader->header.next_related_page, l_pos = 0;
    free(leftHeader);

    size_t record_number = 0;
    index = 0;
    for (uint64_t current = left_data, last = right_search; current != last && index < left_length;) {
        last = current;
        DataPage *dataPage = (DataPage*) malloc(sizeof(DataPage));
        readDataPage(filename, dataPage, current);
        current = dataPage->header.next_related_page;
        page_index = 0;

        for (; page_index < dataPage->header.data_size && index < left_length; page_index += (strlen(dataPage->page_data + page_index) + 1), index++) {
            while (dataPage->page_data[page_index] == '\0' && page_index < PAGE_DATA_SIZE) page_index++;
            if (page_index >= PAGE_DATA_SIZE) {
                if (index > 0) index--;
                break;
            }
            TableRecord *leftRecord = parseTableRecordJSON(dataPage->page_data + page_index, 0, &l_pos, leftSchema);

            switch (applyAll(leftRecord, num_of_l_filters, l_filters)) {
                case FILTER_ACCEPT: {
                    TableRecord *rightRecord = rightTable->firstTableRecord;
                    for (size_t i = 0; i < rightTable->length && rightRecord != NULL; i++) {
                        if (strcmp(leftRecord->dataCells[l_join_index], rightRecord->dataCells[r_join_index]) == 0) {
                            printf("%zd: | ", record_number);

                            for (size_t li = 0; li < leftRecord->length; li++)
                                if (li != l_join_index)
                                    printf("%s | ", leftRecord->dataCells[li]);

                            printf(" << %s >>  | ", leftRecord->dataCells[l_join_index]);

                            for (size_t ri = 0; ri < rightRecord->length; ri++)
                                if (ri != r_join_index)
                                    printf("%s | ", rightRecord->dataCells[ri]);

                            printf("\n");

                            record_number++;
                        }
                        rightRecord = rightRecord->next_record;
                    }
                    break;
                }
                case FILTER_REJECT:
                    break;
                case FILTER_INCOMPATIBLE:
                    printf("%zd: Filter type error\n", index);
                    break;
            }
            destroyTableRecord(leftRecord);
        }
        free(dataPage);
    }

    destroyTable(rightTable);
    destroyTableSchema(leftSchema);
    destroyTableSchema(rightSchema);
}

int deleteRowsPred(const char *filename, const char *table_name, predicate *pred) {
    if (!filename || !table_name) return -1;
    size_t search_result = findTable(filename, table_name);
    if (search_result == SEARCH_TABLE_NOT_FOUND) return -1;

    DataPage *tableHeader = (DataPage*) malloc(sizeof(DataPage));
    readDataPage(filename, tableHeader, search_result);
    size_t length = getTableLength(tableHeader), data = tableHeader->header.next_related_page, pos;
    TableSchema *tableSchema = parseTableSchemaJSON(tableHeader->page_data, 0, &pos);
    free(tableHeader);

    size_t index = 0, page_index, removed = 0, parent_index, expel_candidate;
    for (uint64_t current = data, last = search_result; current != last && index < length;) {
        parent_index = last;
        expel_candidate = current;
        last = current;
        DataPage *dataPage = (DataPage*) malloc(sizeof(DataPage));
        readDataPage(filename, dataPage, current);
        current = dataPage->header.next_related_page;
        page_index = 0;
        for (; page_index < dataPage->header.data_size && index < length; page_index += (strlen(dataPage->page_data + page_index) + 1), index++) {
            while (dataPage->page_data[page_index] == '\0' && page_index < PAGE_DATA_SIZE) page_index++;
            if (page_index >= PAGE_DATA_SIZE) {
                if (index > 0) index--;
                break;
            }
            TableRecord *tableRecord = parseTableRecordJSON(dataPage->page_data + page_index, 0, &pos, tableSchema);

            if (applySingleTablePredicate(tableSchema, tableRecord, pred) == FILTER_ACCEPT) {
                memset(dataPage->page_data + page_index, '\0', pos);
                removed++;
            }

            destroyTableRecord(tableRecord);
        }
        writeDataPage(filename, dataPage);
        free(dataPage);

        if (checkEmpty(filename, expel_candidate)) expelPageFromThread(filename, parent_index, expel_candidate);
    }

    destroyTableSchema(tableSchema);

    tableHeader = (DataPage*) malloc(sizeof(DataPage));
    readDataPage(filename, tableHeader, search_result);
    updateTableLength(tableHeader, getTableLength(tableHeader) - removed);
    writeDataPage(filename, tableHeader);
    free(tableHeader);

    return removed;
}