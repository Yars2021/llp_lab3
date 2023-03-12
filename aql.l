%option noyywrap

%{
#include <stdio.h>
#include "data.h"
#include "aql.tab.h"
%}

integer_reg ([-+]?[0-9]+)
float_reg ([-+]?[0-9]+\.[0-9]+)
boolean_reg (True|False)
string_reg ([a-zA-Z_][a-zA-Z0-9_]*)

%%

"quit"          return QUIT;

"CREATE"        return CREATE;
"DROP"          return DROP;
"TABLE"         return TABLE;

"INTEGER"       {yylval.type = 1; return TYPE;}
"FLOAT"         {yylval.type = 2; return TYPE;}
"BOOLEAN"       {yylval.type = 3; return TYPE;}
"STRING"        {yylval.type = 4; return TYPE;}

"FOR"           return FOR;
"IN"            return IN;
"RETURN"        return RETURN;
"FILTER"        return FILTER;
"INSERT"        return INSERT;
"UPDATE"        return UPDATE;
"REMOVE"        return REMOVE;

"OR"            {yylval.op = 1; return OP;}
"AND"           {yylval.op = 2; return OP;}

"=="            {yylval.cmp = 1; return CMP;}
"!="            {yylval.cmp = 2; return CMP;}
"<>"            {yylval.cmp = 2; return CMP;}
">"             {yylval.cmp = 3; return CMP;}
"<"             {yylval.cmp = 4; return CMP;}
">="            {yylval.cmp = 5; return CMP;}
"<="            {yylval.cmp = 6; return CMP;}
"~"             {yylval.cmp = 7; return CMP;}

":"             return COLON;
";"             return SEMICOLON;
"."             return DOT;
","             return COMMA;
"("             return LBR;
")"             return RBR;
"{"             return LCBR;
"}"             return RCBR;
"'"             return QUOTE;
"*"             return ALL;

{integer_reg}   {sscanf(yytext, "%s", yylval.literal_value); return INTEGER;}
{float_reg}     {sscanf(yytext, "%s", yylval.literal_value); return FLOAT;}
{boolean_reg}   {sscanf(yytext, "%s", yylval.literal_value); return BOOLEAN;}
{string_reg}    {sscanf(yytext, "%s", yylval.literal_value); return STRING;}

[\n]                            /* ignore newlines */;
[ \t]+                          /* ignore whitespace */;

%%
