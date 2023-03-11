#ifndef __AQL_DATA_PACJED_H__
#define __AQL_DATA_PACJED_H__

#include <stdlib.h>

struct AQLDataPacked {
	uint8_t *payload;
	size_t size;
};

struct AQLDataPacked pack_aql_request(char *request_text);
void unpack_aql_request(AQLServiceRequest *request, uint8_t *packed, size_t size);


#endif
