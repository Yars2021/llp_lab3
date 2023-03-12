%{
#include <stdio.h>
#include <stdlib.h>

#include "aql.yy.h"
#include "data.h"
#include "client.h"

extern int yylex();

void yyerror(char*, const char*);
%}

%parse-param {char *address}

%token QUIT
%token CREATE DROP TABLE
%token TYPE INTEGER FLOAT BOOLEAN STRING
%token COLON SEMICOLON DOT COMMA LBR RBR LCBR RCBR QUOTE ALL
%token FOR IN RETURN FILTER INSERT UPDATE REMOVE OP CMP

%type <statement> stmt
%type <statement> create_stmt
%type <statement> select_stmt
%type <statement> insert_stmt
%type <statement> update_stmt
%type <statement> delete_stmt
%type <statement> drop_stmt

%type <type> TYPE
%type <cmp> CMP
%type <op> OP

%type <field> fields
%type <field> field

%type <cell> cells
%type <cell> cell

%type <table_var> loops
%type <table_var> loop

%type <pred> predicate

%type <reference> references
%type <reference> reference

%type <literal_value> INTEGER
%type <literal_value> FLOAT
%type <literal_value> BOOLEAN
%type <literal_value> STRING
%type <literal> literal

%left OR
%left AND

%union {
    int type;
    int op;
    int cmp;
    char literal_value[512];
    statement *statement;
    field *field;
    cell *cell;
    table_var_link *table_var;
    predicate *pred;
    reference *reference;
    literal *literal;
}

%%

input:
|       input stmt SEMICOLON {send_and_receive(address, $2); free_statement($2); printf("\n");}
|       input QUIT {printf("Session ended\n"); exit(0);}
;

stmt:   create_stmt
|       select_stmt
|       insert_stmt
|       update_stmt
|       delete_stmt
|       drop_stmt
;

create_stmt:    CREATE TABLE STRING LBR fields RBR {$$ = create_create_statement($3, $5);}
;

select_stmt:    loop RETURN references {$$ = create_select_statement(0, $1, NULL, $3);}
|               loop RETURN ALL {$$ = create_select_statement(0, $1, NULL, NULL);}
|               loop FILTER predicate RETURN references {$$ = create_select_statement(1, $1, $3, $5);}
|               loop FILTER predicate RETURN ALL {$$ = create_select_statement(1, $1, $3, NULL);}
|               loops FILTER predicate RETURN references {$$ = create_select_statement(2, $1, $3, $5);}
|               loops FILTER predicate RETURN ALL {$$ = create_select_statement(2, $1, $3, NULL);}
;

insert_stmt:    INSERT LCBR cells RCBR IN STRING {$$ = create_insert_statement($6, $3);}
;

update_stmt:    loop UPDATE LCBR cells RCBR IN STRING {$$ = create_update_statement($7, $4, NULL, $1);}
|               loop FILTER predicate UPDATE LCBR cells RCBR IN STRING {$$ = create_update_statement($9, $6, $3, $1);}
;

delete_stmt:    loop REMOVE STRING IN STRING {$$ = create_delete_statement($5, NULL, $1);}
|               loop FILTER predicate REMOVE STRING IN STRING {$$ = create_delete_statement($7, $3, $1);}
;

drop_stmt:  DROP TABLE STRING {$$ = create_drop_statement($3);}
;

fields: field COMMA fields {$$ = $1; $1->next = $3;}
|       field
;

field:  STRING COLON TYPE {$$ = create_field($1, $3);}
;

cells: cell COMMA cells {$$ = $1; $1->next = $3;}
|      cell
;

cell:  STRING COLON INTEGER {$$ = create_cell($1, $3);}
|      STRING COLON FLOAT {$$ = create_cell($1, $3);}
|      STRING COLON BOOLEAN {$$ = create_cell($1, $3);}
|      STRING COLON QUOTE STRING QUOTE {$$ = create_cell($1, $4);}
;

loops:  loop loop {$$ = $1; $1->next = $2;}
;

loop:   FOR STRING IN STRING {$$ = create_table_var_link($2, $4);}
;

predicate:  predicate OP predicate {$$ = create_complex_predicate($2, $1, $3, 0);}
|           predicate OP LBR predicate RBR {$$ = create_complex_predicate($2, $1, $4, 1);}
|           reference CMP reference {$$ = create_simple_predicate(1, 1, $2, NULL, NULL, $1, $3);}
|           reference CMP literal {$$ = create_simple_predicate(1, 2, $2, NULL, $3, $1, NULL);}
|           literal CMP reference {$$ = create_simple_predicate(2, 1, $2, $1, NULL, NULL, $3);}
;

references: reference COMMA references {$$ = $1; $1->next = $3;}
|           reference
;

reference: STRING DOT STRING {$$ = create_reference($1, $3);}
;

literal:    INTEGER {$$ = create_literal($1, 0);}
|           FLOAT {$$ = create_literal($1, 1);}
|           BOOLEAN {$$ = create_literal($1, 2);}
|           QUOTE STRING QUOTE {$$ = create_literal($2, 3);}
;

%%

void yyerror (char *address, const char *str) {
	fprintf(stderr, "error: %s\n", str);
}

// tcp://127.0.0.1:7777
int main(int argc, char **argv) {
	if (argc != 2) {
	    printf("Invalid arguments\nAn address should be passed (example: tcp://127.0.0.1:7777)");
	    return -1;
	}

    printf("AQL client.\n");
    printf("  Type query and press 'Enter'\n");
    printf("  Type 'quit' to exit\n\n");

	yyparse(argv[1]);

	return 0;
}