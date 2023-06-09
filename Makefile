all:
	flex -o aql.lex.c aql.l
	bison -o aql.tab.c -vd aql.y
	protoc --c_out=. ./aql.proto
	cc -o aqls ./aql.pb-c.c ./common.c ./data.c ./aqls.c ./lib_database/db_internals.c ./lib_database/db_file_manager.c ./lib_database/db_interface.c -lnanomsg -lprotobuf-c -O3
	cc -o aqlc ./aql.pb-c.c ./common.c ./aql.lex.c ./aql.tab.c ./data.c ./client.c -lm -ll -lnanomsg -lprotobuf-c -O3

clean:
	rm -f ./aql.lex.c ./aql.tab.* ./aql.output ./aqlc ./aqls ./aql.pb-c.* *.c~ *.h~ Makefile~
