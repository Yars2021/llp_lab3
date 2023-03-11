all:
	protoc --c_out=. ./aql.proto
	cc -o aqls ./aql.pb-c.c ./common.c ./aqls.c -lnanomsg -lprotobuf-c -O3
	cc -o aqlc ./aql.pb-c.c ./common.c ./aqlc.c -lnanomsg -lprotobuf-c -O3
clean:
	rm -f ./aqlc ./aqls aql.pb-c.* *.c~ *.h~ Makefile~
