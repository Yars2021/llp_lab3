#include <stdio.h>
#include <protobuf-c/protobuf-c.h>
#include "aql.pb-c.h"
#include "common.h"

struct AQLDataPacked pack_aql_request(char *request_text) {
	struct AQLDataPacked packed;
	/* Packing string to Protobuf structure */
    AQLServiceRequest request;
    aqlservice_request__init(&request);
    request.payload = request_text;
    
    packed.size = aqlservice_request__get_packed_size(&request);
    packed.payload = (uint8_t *)malloc(packed.size);
    aqlservice_request__pack(&request, packed.payload);
	return packed;
}

void unpack_aql_request(AQLServiceRequest *request, uint8_t *packed, size_t size) {
	printf("packed: %ld, size: %ld\n", sizeof(packed), size);

	request = aqlservice_request__unpack(NULL, size, packed);

	if (request == NULL) printf("UNPACKED IS NULL\n");
    //aqlservice__free_unpacked(rev, NULL);
}
