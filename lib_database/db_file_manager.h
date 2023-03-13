//
// Created by yars on 17.10.22.
//

#ifndef LLP_LAB1_C_DB_FILE_MANAGER_H
#define LLP_LAB1_C_DB_FILE_MANAGER_H

#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include "db_internals.h"
#include "db_interface.h"

#define PAGE_SIZE 4096
#define PAGE_HEADER_SIZE 256
#define PAGE_METADATA_SIZE 237
#define PAGE_E_HEADER_MD_SIZE 229
#define PAGE_DATA_SIZE 3840

#define PAGE_CORRUPT_EXITCODE       (-1)
#define PAGE_TYPE_TABLE_DATA        0b00000000
#define PAGE_TYPE_TABLE_HEADER      0b00000001
#define PAGE_TYPE_DATABASE_HEADER   0b00000010
#define PAGE_STATUS_INACTIVE        0b00000000
#define PAGE_STATUS_ACTIVE          0b10000000
#define PAGE_TYPE_MASK              0b00000011
#define PAGE_STATUS_MASK            0b10000000

#define PAGE_SEARCH_FAILED 0
#define PAGE_DB_ROOT_INDEX 0

#define SEARCH_PAGE_NOT_FOUND (-1)
#define SEARCH_TABLE_NOT_FOUND 0

/*
 * DataPage flags:
 *
 * Counting from lowest to highest bits:
 * 0 and 1 bits - page type
 * 7 bit - page status (ACTIVE - page is currently used by the db, INACTIVE - free page)
 *
 *
 * Page types (M - the field is stored in the metadata):
 *
 * Flags    | Name               | Description
 * ---------------------------------------------------------------------------
 * 00       | TableDataPage      | Table data page, contains records.
 * ---------------------------------------------------------------------------
 * 01       | TableHeaderPage    | Table header page, contains
 *          |                    | table length (4 bytes) (M),
 *          |                    | max id in the table (4 bytes) (M),
 *          |                    | table name (M) and table schema.
 * ---------------------------------------------------------------------------
 * 10       | DatabaseHeaderPage | Database header page, contains
 *          |                    | number of pages (4 bytes) (M),
 *          |                    | number of tables (4 bytes) (M),
 *          |                    | db name (M) and first page
 *          |                    | indexes for every table.
 */

/// Header of the DataPage. Contains flags (1 byte), page_index (8 bytes), next_related_page (8 bytes),
/// data_size (2 bytes) and metadata (236 bytes) + 1 termination byte.
/// If the page is the last one in the relation thread, its next_related_page field will be equal to its own index.
typedef struct {
    u_int8_t flags;
    u_int64_t page_index;
    u_int64_t next_related_page;
    u_int16_t data_size;
    char metadata[PAGE_METADATA_SIZE];
} __attribute__((packed)) DataPageHeader;

/// DataPage. Its size is 4095 bytes + 1 termination byte, 256 first of which store the header data.
typedef struct {
    DataPageHeader header;
    char page_data[PAGE_SIZE - PAGE_HEADER_SIZE];
} __attribute__((packed)) DataPage;

/// Clears the page (does not affect its index).
void freePage(DataPage *dataPage);

/// Clears the page and all the related pages in the file.
void freePageThread(const char *filename, size_t page_index);

/// Clears the database file.
void freeDatabaseFile(const char *filename);

/// Creates the DatabaseHeaderPage and places it in the file.
void createDatabasePage(const char *filename, const char *db_name);

/// Appends a new empty page to the file.
void expandDBFile(const char *filename);

/// Appends a page to a page thread linking it to the previous one. Returns the new page index.
/// Page flags are copied.
size_t expandPageThread(const char *filename, size_t page_index);

/// Returns the index of the closest free page starting from the starting_point.
/// If such page cannot be found, it creates a new one.
size_t findFreePageOrExpand(const char *filename, size_t starting_point);

/// Extracts a DataPage by its number from a file and puts it into a struct.
void readDataPage(const char *filename, DataPage *dataPage, size_t page_index);

/// Writes a DataPage struct into a file.
void writeDataPage(const char *filename, DataPage *dataPage);

/// Updates the metadata field value in the DataPages header.
void updatePageMetadata(DataPage *dataPage, const char *metadata);

/// Updates the data of the DataPage.
void updatePageData(DataPage *dataPage, const char *data);

/// Updates the metadata without affecting the first 8 bytes, which can store additional fields
/// if the page is a HeaderPage.
void updateHeaderPageMetadata(DataPage *dataPage, const char *metadata);

/// Updates the number of pages for the DB Header page (stored in the first 4 bytes of the metadata).
void updateNumberOfPages(DataPage *dataPage, u_int32_t num);

/// Updates the number of pages for the DB Header page (stored in the second 4 bytes of the metadata).
void updateNumberOfTables(DataPage *dataPage, u_int32_t num);

/// Updates the number of records for the Table Header page (stored in the first 4 bytes of the metadata).
void updateTableLength(DataPage *dataPage, u_int32_t num);

/// Updates the number of records for the Table Header page (stored in the second 4 bytes of the metadata).
void updateTableMaxID(DataPage *dataPage, u_int32_t num);

/// Returns a char pointer to the DataPages metadata.
char *getPageMetadata(DataPage *dataPage);

/// Returns a char pointer to the DataPages data.
char *getPageData(DataPage *dataPage);

/// Returns a char pointer to the Header pages metadata.
char *getHeaderPageMetadata(DataPage *dataPage);

/// Returns the number of pages in the file.
int32_t getNumberOfPages(DataPage *dataPage);

/// Returns the number of tables in the database.
int32_t getNumberOfTables(DataPage *dataPage);

/// Returns the length of the table.
int32_t getTableLength(DataPage *dataPage);

/// returns max ID of the table.
int32_t getTableMaxID(DataPage *dataPage);

/// Returns the type of the DataPage or PAGE_CORRUPT_EXITCODE.
int getPageType(DataPage *dataPage);

/// Returns the status of the DataPage or PAGE_CORRUPT_EXITCODE.
int getPageStatus(DataPage *dataPage);

/// Updates the DataPages type.
void updatePageType(DataPage *dataPage, u_int8_t new_type);

/// Updates the DataPages status.
void updatePageStatus(DataPage *dataPage, u_int8_t new_status);

/// Appends a line to the page.
void appendData(DataPage *dataPage, const char *line);

/// Appends a line to the page in file.
/// If there is not enough space on the page, expands a page thread and writes the data on the new page.
void appendDataOrExpandThread(const char *filename, size_t page_index, const char *line);

/// Returns the TableHeader page index for the provided table name. If the table does not exist, returns PAGE_SEARCH_FAILED.
size_t findTableOnPage(DataPage *dataPage, const char *table_name, size_t *checked);

/// Returns the TableHeader page index for the provided table name. If the table does not exist, returns PAGE_SEARCH_FAILED.
size_t findTable(const char *filename, const char *table_name);

/// Returns the schema of the table.
TableSchema *getSchema(const char *filename, const char *table_name);

/// Adds a new table header to the file and creates a link for it in the root page. Exits if the table already exists.
/// (CREATE TABLE ... VALUES(...)).
int addTableHeader(const char *filename, Table *table);

/// Appends all the records into the table. Does nothing if the table does not exist.
/// (INSERT INTO ... VALUES(...)).
void insertTableRecords(const char *filename, Table *table);

/// Helper function for deleteTable(). Finds the index, where the TableLink with the provided name is stored and erases it.
size_t findAndErase(DataPage *dataPage, const char *table_name, size_t *checked);

/// Searches for the table and updates its Max ID.
void findAndUpdateMaxID(const char *filename, const char *table_name, uint32_t new_max_id);

/// Searches for the table and returns its Max ID.
uint32_t findAndGetMaxID(const char *filename, const char *table_name);

/// Clears all the pages with the table data and erases the TableLink.
/// (DROP TABLE ...).
int deleteTable(const char *filename, const char *table_name);

/// Outputs all the records of the table filtering them through the provided filter.
/// (SELECT * FROM ... WHERE ...)
void printTable(const char *filename, const char *table_name, size_t num_of_filters, SearchFilter **filters);

/// Updates filtered rows.
/// (UPDATE ... FROM ... WHERE ...)
void updateRows(const char *filename, char *table_name, TableRecord *new_value, size_t num_of_filters, SearchFilter **filters);

/// Returns 1 if the page is empty (contains only \0 chars).
int checkEmpty(const char *filename, size_t page_index);

/// Expels a page from its thread, linking its parent to the next one or to itself if the expelled page was the tail of the thread.
void expelPageFromThread(const char *filename, size_t parent_page, size_t page_index);

/// Deletes filtered rows. If the removal empties a page, it gets expelled from the thread and can be reused later.
/// (DELETE FROM ... WHERE ...)
void deleteRows(const char *filename, const char *table_name, size_t num_of_filters, SearchFilter **filters);

/// Merges 2 tables into one in runtime (with filters) and outputs the result.
/// (SELECT FROM ... INNER JOIN ... ON ... WHERE ...)
void innerJoinSelect(const char *filename, const char *left_table, const char *right_table, size_t l_join_index, size_t r_join_index, size_t num_of_l_filters, SearchFilter **l_filters, size_t num_of_r_filters, SearchFilter **r_filters);

#endif //LLP_LAB1_C_DB_FILE_MANAGER_H