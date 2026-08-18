package com.bakdata.kafka;

import static com.bakdata.kafka.ByteFlagLargeMessagePayloadProtocol.stripFlag;
import static com.bakdata.kafka.LargeMessagePayload.ofBytes;
import static com.bakdata.kafka.LargeMessagePayload.ofUri;
import static com.bakdata.kafka.LargeMessageRetrievingClient.deserializeUri;

import lombok.experimental.UtilityClass;
import org.apache.kafka.common.header.internals.RecordHeaders;

@UtilityClass
class TestHelper {
    private static final LargeMessagePayloadProtocol BYTE_FLAG_PROTOCOL = new ByteFlagLargeMessagePayloadProtocol();

    static BlobStorageURI deserializeUriWithFlag(final byte[] data) {
        final byte[] uriBytes = stripFlag(data);
        return deserializeUri(uriBytes);
    }

    static byte[] serializeUri(final String uri) {
        return BYTE_FLAG_PROTOCOL.serialize(ofUri(uri), new RecordHeaders(), false);
    }

    static byte[] serialize(final byte[] bytes) {
        return BYTE_FLAG_PROTOCOL.serialize(ofBytes(bytes), new RecordHeaders(), false);
    }
}
