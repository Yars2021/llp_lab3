//
// Created by yars on 01.03.23.
//

#ifndef AQLPARSER_DATA_H
#define AQLPARSER_DATA_H

typedef struct {
    int type;
    char *value;
} literal;

typedef struct field {
    int type;
    char *name;
    struct field *next;
} field;

typedef struct cell {
    char *name;
    char *value;
    struct cell *next;
} cell;

typedef struct table_var_link {
    char *table;
    char *var;
    struct table_var_link *next;
} table_var_link;

typedef struct reference {
    char *table;
    char *field;
    struct reference *next;
} reference;

typedef struct predicate {
    int l_type; // 0 - predicate, 1 - reference, 2 - literal
    int r_type; // 0 - predicate, 1 - reference, 2 - literal
    int cmp_type; // 1 - =, 2 - !=, 3 - >, 4 - <, 5 - >=, 6 - <=, 7 - ~
    int op_type; // 1 - OR, 2 - AND
    int priority; // 0 - left, 1 - right
    union {
        struct predicate *left;
        reference *l_ref;
        literal *l_lit;
    };
    union {
        struct predicate *right;
        reference *r_ref;
        literal *r_lit;
    };
} predicate;

typedef struct {
    field *field_list;
} create_stmt;

typedef struct {
    int type; // 0 - unfiltered, 1 - filtered, 2 - joined
    table_var_link *names;
    predicate *pred;
    reference *ref_list;
} select_stmt;

typedef struct {
    cell *cell_list;
} insert_stmt;

typedef struct {
    cell *cell_list;
    predicate *pred;
    table_var_link *names;
} update_stmt;

typedef struct {
    predicate *pred;
    table_var_link *names;
} delete_stmt;

/// stmt_type: 0 - create, 1 - select, 2 - insert, 3 - update, 4 - delete, 5 - drop
typedef struct {
    int stmt_type;
    char *table;
    union {
        create_stmt *create_stmt;
        select_stmt *select_stmt;
        insert_stmt *insert_stmt;
        update_stmt *update_stmt;
        delete_stmt *delete_stmt;
    };
} statement;

literal *create_literal(char *value, int type);
field *create_field(char *name, int type);
cell *create_cell(char *name, char *value);
table_var_link *create_table_var_link(char *var, char *table);
reference *create_reference(char *table, char *f);
predicate *create_simple_predicate(int l_type, int r_type, int cmp_type, literal *l_left, literal *l_right, reference *r_left, reference *r_right);
predicate *create_complex_predicate(int op_type, predicate *left, predicate *right, int priority);
statement *create_create_statement(char *table, field *fields);
statement *create_select_statement(int type, table_var_link *names, predicate *pred, reference *ref_list);
statement *create_insert_statement(char *table, cell *cells);
statement *create_update_statement(char *table, cell *cells, predicate *pred, table_var_link *names);
statement *create_delete_statement(char *table, predicate *pred, table_var_link *names);
statement *create_drop_statement(char *table);
void print_predicate(predicate *pred, int tabs);
void print_statement(statement *stmt);
char *serialize_predicate(predicate *pred);
char *serialize_statement(statement *stmt);
void free_predicate(predicate *pred);
void free_statement(statement *stmt);

#endif //AQLPARSER_DATA_H