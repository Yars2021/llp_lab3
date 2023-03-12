#include "data.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

literal *create_literal(char *value, int type) {
    literal *lit = (literal*) malloc(sizeof(literal));
    if (lit) {
        lit->value = (char*) malloc(strlen(value) + 1);
        memset(lit->value, 0, strlen(value) + 1);
        strcpy(lit->value, value);
        lit->type = type;
    }
    return lit;
}

field *create_field(char *name, int type) {
    field *f = (field*) malloc(sizeof(field));
    if (f) {
        f->name = (char*) malloc(strlen(name) + 1);
        memset(f->name, 0, strlen(name) + 1);
        strcpy(f->name, name);
        f->type = type;
        f->next = NULL;
    }
    return f;
}

cell *create_cell(char *name, char *value) {
    cell *c = (cell*) malloc(sizeof(cell));
    if (c) {
        c->name = (char*) malloc(strlen(name) + 1);
        memset(c->name, 0, strlen(name) + 1);
        strcpy(c->name, name);
        c->value = (char*) malloc(strlen(value) + 1);
        memset(c->value, 0, strlen(value) + 1);
        strcpy(c->value, value);
        c->next = NULL;
    }
    return c;
}

table_var_link *create_table_var_link(char *var, char *table) {
    table_var_link *link = (table_var_link*) malloc(sizeof(table_var_link));
    if (link) {
        link->var = (char*) malloc(strlen(var) + 1);
        memset(link->var, 0, strlen(var) + 1);
        strcpy(link->var, var);
        link->table = (char*) malloc(strlen(table) + 1);
        memset(link->table, 0, strlen(table) + 1);
        strcpy(link->table, table);
        link->next = NULL;
    }
    return link;
}

reference *create_reference(char *table, char *f) {
    reference *ref = (reference*) malloc(sizeof(reference));
    if (ref) {
        ref->table = (char*) malloc(strlen(table) + 1);
        memset(ref->table, 0, strlen(table) + 1);
        strcpy(ref->table, table);
        ref->field = (char*) malloc(strlen(f) + 1);
        memset(ref->field, 0, strlen(f) + 1);
        strcpy(ref->field, f);
        ref->next = NULL;
    }
    return ref;
}

predicate *create_simple_predicate(int l_type, int r_type, int cmp_type, literal *l_left, literal *l_right, reference *r_left, reference *r_right) {
    predicate *pred = (predicate*) malloc(sizeof(predicate));
    if (pred) {
        pred->l_type = l_type;
        pred->r_type = r_type;
        pred->cmp_type = cmp_type;
        switch (pred->l_type) {
            case 1:
                pred->l_ref = r_left;
                break;
            case 2:
                pred->l_lit = l_left;
                break;
        }
        switch (pred->r_type) {
            case 1:
                pred->r_ref = r_right;
                break;
            case 2:
                pred->r_lit = l_right;
                break;
        }
    }
    return pred;
}

predicate *create_complex_predicate(int op_type, predicate *left, predicate *right, int priority) {
    predicate *pred = (predicate*) malloc(sizeof(predicate));
    if (pred) {
        pred->l_type = 0;
        pred->r_type = 0;
        pred->op_type = op_type;
        pred->left = left;
        pred->right = right;
        pred->priority = priority;
    }
    return pred;
}

statement *create_create_statement(char *table, field *fields) {
    statement *stmt = (statement*) malloc(sizeof(statement));
    if (stmt) {
        stmt->table = (char*) malloc(strlen(table) + 1);
        memset(stmt->table, 0, strlen(table) + 1);
        strcpy(stmt->table, table);
        stmt->stmt_type = 0;
        stmt->create_stmt = (create_stmt*) malloc(sizeof(create_stmt));
        stmt->create_stmt->field_list = fields;
    }
    return stmt;
}

statement *create_select_statement(int type, table_var_link *names, predicate *pred, reference *ref_list) {
    statement *stmt = (statement*) malloc(sizeof(statement));
    if (stmt) {
        stmt->table = (char*) malloc(1);
        stmt->table[0] = '\0';
        stmt->stmt_type = 1;
        stmt->select_stmt = (select_stmt*) malloc(sizeof(select_stmt));
        stmt->select_stmt->type = type;
        stmt->select_stmt->names = names;
        stmt->select_stmt->pred = pred;
        stmt->select_stmt->ref_list = ref_list;
    }
    return stmt;
}

statement *create_insert_statement(char *table, cell *cells) {
    statement *stmt = (statement*) malloc(sizeof(statement));
    if (stmt) {
        stmt->table = (char*) malloc(strlen(table) + 1);
        memset(stmt->table, 0, strlen(table) + 1);
        strcpy(stmt->table, table);
        stmt->stmt_type = 2;
        stmt->insert_stmt = (insert_stmt*) malloc(sizeof(insert_stmt));
        stmt->insert_stmt->cell_list = cells;
    }
    return stmt;
}

statement *create_update_statement(char *table, cell *cells, predicate *pred, table_var_link *names) {
    statement *stmt = (statement*) malloc(sizeof(statement));
    if (stmt) {
        stmt->table = (char*) malloc(strlen(table) + 1);
        memset(stmt->table, 0, strlen(table) + 1);
        strcpy(stmt->table, table);
        stmt->stmt_type = 3;
        stmt->update_stmt = (update_stmt*) malloc(sizeof(update_stmt));
        stmt->update_stmt->cell_list = cells;
        stmt->update_stmt->names = names;
        stmt->update_stmt->pred = pred;
    }
    return stmt;
}

statement *create_delete_statement(char *table, predicate *pred, table_var_link *names) {
    statement *stmt = (statement*) malloc(sizeof(statement));
    if (stmt) {
        stmt->table = (char*) malloc(strlen(table) + 1);
        memset(stmt->table, 0, strlen(table) + 1);
        strcpy(stmt->table, table);
        stmt->stmt_type = 4;
        stmt->delete_stmt = (delete_stmt*) malloc(sizeof(delete_stmt));
        stmt->delete_stmt->names = names;
        stmt->delete_stmt->pred = pred;
    }
    return stmt;
}

statement *create_drop_statement(char *table) {
    statement *stmt = (statement*) malloc(sizeof(statement));
    if (stmt) {
        stmt->table = (char*) malloc(strlen(table) + 1);
        memset(stmt->table, 0, strlen(table) + 1);
        strcpy(stmt->table, table);
        stmt->stmt_type = 5;
    }
    return stmt;
}

void print_predicate(predicate *pred, int tabs) {
    if (pred) {
        char *whitespace = (char*) malloc(tabs + 1);
        memset(whitespace, 0, tabs + 1);
        memset(whitespace, '\t', tabs);

        char *l_type_s, *r_type_s, *cmp_type_s, *op_type_s;

        switch (pred->l_type) {
            case 0:
                l_type_s = "predicate";
                break;
            case 1:
                l_type_s = "reference";
                break;
            case 2:
                l_type_s = "predicate";
                break;
        }

        switch (pred->r_type) {
            case 0:
                r_type_s = "predicate";
                break;
            case 1:
                r_type_s = "reference";
                break;
            case 2:
                r_type_s = "predicate";
                break;
        }

        switch (pred->cmp_type) {
            case 0:
                cmp_type_s = "";
                break;
            case 1:
                cmp_type_s = "==";
                break;
            case 2:
                cmp_type_s = "!=";
                break;
            case 3:
                cmp_type_s = ">";
                break;
            case 4:
                cmp_type_s = "<";
                break;
            case 5:
                cmp_type_s = ">=";
                break;
            case 6:
                cmp_type_s = "<=";
                break;
            case 7:
                cmp_type_s = "~";
                break;
        }

        switch (pred->op_type) {
            case 0:
                op_type_s = "";
                break;
            case 1:
                op_type_s = "OR";
                break;
            case 2:
                op_type_s = "AND";
                break;
        }

        printf("%sl_type: %s\n%sr_type: %s\n", whitespace, l_type_s, whitespace, r_type_s);
        printf("%scmp_type: %s\n%sop_type: %s\n", whitespace, cmp_type_s, whitespace, op_type_s);
        printf("%spriority: %d\n%sleft:\n%s{\n", whitespace, pred->priority, whitespace, whitespace);

        switch (pred->l_type) {
            case 0: {
                print_predicate(pred->left, tabs + 1);
                break;
            }
            case 1: {
                printf("%s\ttable: %s\n%s\tfield: %s\n", whitespace, pred->l_ref->table, whitespace, pred->l_ref->field);
                break;
            }
            case 2: {
                char *l_type;
                switch (pred->l_lit->type) {
                    case 0:
                        l_type = "INTEGER";
                        break;
                    case 1:
                        l_type = "FLOAT";
                        break;
                    case 2:
                        l_type = "BOOLEAN";
                        break;
                    case 3:
                        l_type = "STRING";
                        break;
                }
                printf("%s\ttype: %s\n%s\tvalue: %s\n", whitespace, l_type, whitespace, pred->l_lit->value);
                break;
            }
        }

        printf("%s}\n%sright:\n%s{\n", whitespace, whitespace, whitespace);

        switch (pred->r_type) {
            case 0: {
                print_predicate(pred->right, tabs + 1);
                break;
            }
            case 1: {
                printf("%s\ttable: %s\n%s\tfield: %s\n", whitespace, pred->r_ref->table, whitespace, pred->r_ref->field);
                break;
            }
            case 2: {
                char *r_type;
                switch (pred->r_lit->type) {
                    case 0:
                        r_type = "INTEGER";
                        break;
                    case 1:
                        r_type = "FLOAT";
                        break;
                    case 2:
                        r_type = "BOOLEAN";
                        break;
                    case 3:
                        r_type = "STRING";
                        break;
                }
                printf("%s\ttype: %s\n%s\tvalue: %s\n", whitespace, r_type, whitespace, pred->r_lit->value);
                break;
            }
        }

        printf("%s}\n", whitespace);

        free(whitespace);
    }
}

void print_statement(statement *stmt) {
    char *type;
    if (stmt) {
        switch (stmt->stmt_type) {
            case 0:
                type = "create";
                break;
            case 1:
                type = "select";
                break;
            case 2:
                type = "insert";
                break;
            case 3:
                type = "update";
                break;
            case 4:
                type = "delete";
                break;
            case 5:
                type = "drop";
                break;
        }
        printf("\n{\n\tstmt_type: %s\n\ttable: %s\n", type, stmt->table);

        switch (stmt->stmt_type) {
            case 0: {
                printf("\tfields:\n\t[\n");
                for (field *f = stmt->create_stmt->field_list; f != NULL; f = f->next) {
                    switch (f->type) {
                        case 1:
                            type = "INTEGER";
                            break;
                        case 2:
                            type = "FLOAT";
                            break;
                        case 3:
                            type = "BOOLEAN";
                            break;
                        case 4:
                            type = "STRING";
                            break;
                    }
                    printf("\t\t{\n\t\t\tname: %s\n\t\t\ttype: %s\n\t\t}", f->name, type);
                    if (f->next != NULL) printf(",");
                    printf("\n");
                }
                printf("\t]\n");
                break;
            }
            case 1: {
                printf("\tsel_type: %s\n", stmt->select_stmt->type == 0 ? "unfiltered" : stmt->select_stmt->type == 1 ? "filtered" : "joined");
                printf("\tvariables:\n\t[\n");
                for (table_var_link *var = stmt->select_stmt->names; var != NULL; var = var->next) {
                    printf("\t\t{\n\t\t\ttable: %s\n\t\t\tvar: %s\n\t\t}", var->table, var->var);
                    if (var->next != NULL) printf(",");
                    printf("\n");
                }
                printf("\t]\n\treturn:\n\t[\n");
                for (reference *ref = stmt->select_stmt->ref_list; ref != NULL; ref = ref->next) {
                    printf("\t\t{\n\t\t\ttable: %s\n\t\t\tfield: %s\n\t\t}", ref->table, ref->field);
                    if (ref->next != NULL) printf(",");
                    printf("\n");
                }
                printf("\t]\n\tpredicate:\n\t{\n");
                print_predicate(stmt->select_stmt->pred, 2);
                printf("\t}\n");
                break;
            }
            case 2: {
                printf("\tcells:\n\t[\n");
                for (cell *c = stmt->insert_stmt->cell_list; c != NULL; c = c->next) {
                    printf("\t\t{\n\t\t\tname: %s\n\t\t\tvalue: %s\n\t\t}", c->name, c->value);
                    if (c->next != NULL) printf(",");
                    printf("\n");
                }
                printf("\t]\n");
                break;
            }
            case 3: {
                printf("\tvariables:\n\t[\n");
                for (table_var_link *var = stmt->update_stmt->names; var != NULL; var = var->next) {
                    printf("\t\t{\n\t\t\ttable: %s\n\t\t\tvar: %s\n\t\t}", var->table, var->var);
                    if (var->next != NULL) printf(",");
                }
                printf("\n\t]\n");
                printf("\tcells:\n\t[\n");
                for (cell *c = stmt->update_stmt->cell_list; c != NULL; c = c->next) {
                    printf("\t\t{\n\t\t\tname: %s\n\t\t\tvalue: %s\n\t\t}", c->name, c->value);
                    if (c->next != NULL) printf(",");
                    printf("\n");
                }
                printf("\t]\n\tpredicate:\n\t{\n");
                print_predicate(stmt->update_stmt->pred, 2);
                printf("\t}\n");
                break;
            }
            case 4: {
                printf("\tvariables:\n\t[\n");
                for (table_var_link *var = stmt->delete_stmt->names; var != NULL; var = var->next) {
                    printf("\t\t{\n\t\t\ttable: %s\n\t\t\tvar: %s\n\t\t}", var->table, var->var);
                    if (var->next != NULL) printf(",");
                    printf("\n");
                }
                printf("\t]\n");
                printf("\tpredicate:\n\t{\n");
                print_predicate(stmt->delete_stmt->pred, 2);
                printf("\t}\n");
                break;
            }
        }

        printf("}\n");
    }
}

void free_predicate(predicate *pred) {
    if (pred) {
        if (pred->l_type == 0 && pred->r_type == 0) {
            free_predicate(pred->left);
            free_predicate(pred->right);
        } else {
            if (pred->l_type == 1) {
                free(pred->l_ref->field);
                free(pred->l_ref->table);
                free(pred->l_ref);
            } else {
                free(pred->l_lit->value);
                free(pred->l_lit);
            }
            if (pred->r_type == 1) {
                free(pred->r_ref->field);
                free(pred->r_ref->table);
                free(pred->r_ref);
            } else {
                free(pred->r_lit->value);
                free(pred->r_lit);
            }
        }
    }
}

void free_statement(statement *stmt) {
    if (stmt) {
        free(stmt->table);

        switch (stmt->stmt_type) {
            case 0: {
                for (field *f = stmt->create_stmt->field_list; f != NULL; f = f->next)
                    free(f->name);
                free(stmt->create_stmt);
                break;
            }
            case 1: {
                for (table_var_link *var = stmt->select_stmt->names; var != NULL;) {
                    table_var_link *curr = var;
                    var = var->next;
                    free(curr->var);
                    free(curr->table);
                    free(curr);
                }
                for (reference *ref = stmt->select_stmt->ref_list; ref != NULL;) {
                    reference *curr = ref;
                    ref = ref->next;
                    free(curr->table);
                    free(curr->field);
                    free(curr);
                }
                free_predicate(stmt->select_stmt->pred);
                free(stmt->select_stmt);
                break;
            }
            case 2: {
                for (cell *c = stmt->insert_stmt->cell_list; c != NULL;) {
                    cell *curr = c;
                    c = c->next;
                    free(curr->value);
                    free(curr->name);
                    free(curr);
                }
                free(stmt->insert_stmt);
                break;
            }
            case 3: {
                for (table_var_link *var = stmt->update_stmt->names; var != NULL;) {
                    table_var_link *curr = var;
                    var = var->next;
                    free(curr->var);
                    free(curr->table);
                    free(curr);
                }
                for (cell *c = stmt->update_stmt->cell_list; c != NULL;) {
                    cell *curr = c;
                    c = c->next;
                    free(curr->value);
                    free(curr->name);
                    free(curr);
                }
                free_predicate(stmt->update_stmt->pred);
                free(stmt->update_stmt);
                break;
            }
            case 4: {
                for (table_var_link *var = stmt->delete_stmt->names; var != NULL;) {
                    table_var_link *curr = var;
                    var = var->next;
                    free(curr->var);
                    free(curr->table);
                    free(curr);
                }
                free_predicate(stmt->delete_stmt->pred);
                free(stmt->delete_stmt);
                break;
            }
        }

        free(stmt);
    }
}