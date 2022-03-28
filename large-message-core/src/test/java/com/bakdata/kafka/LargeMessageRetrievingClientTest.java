/*
 * MIT License
 *
 * Copyright (c) 2022 bakdata
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.bakdata.kafka;

import static com.bakdata.kafka.FlagHelper.IS_BACKED;
import static com.bakdata.kafka.FlagHelper.IS_NOT_BACKED;
import static com.bakdata.kafka.HeaderLargeMessagePayloadProtocol.getHeaderName;
import static com.bakdata.kafka.LargeMessagePayload.ofBytes;
import static com.bakdata.kafka.LargeMessagePayload.ofUri;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.when;

import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class LargeMessageRetrievingClientTest {

    private static final Serializer<String> STRING_SERIALIZER = Serdes.String().serializer();
    private static final LargeMessagePayloadProtocol BYTE_FLAG_PROTOCOL = new ByteFlagLargeMessagePayloadProtocol();
    @Mock
    private BlobStorageClient client;

    static Stream<Arguments> generateHeaders() {
        return Stream.<Function<Boolean, Headers>>of(
                bool -> new RecordHeaders(),
                LargeMessageRetrievingClientTest::nonBackedHeaders,
                LargeMessageRetrievingClientTest::backedHeaders
        ).flatMap(factory -> Stream.of(false, true)
                .map(bool -> Arguments.of(factory.apply(bool), bool))
        );
    }

    static byte[] serializeUri(final String uri) {
        return BYTE_FLAG_PROTOCOL.serialize(ofUri(uri), new RecordHeaders(), false);
    }

    private static void assertHasHeader(final Headers headers, final boolean isKey) {
        assertThat(headers.headers(getHeaderName(isKey))).hasSize(1);
    }

    private static byte[] serialize(final byte[] bytes) {
        return BYTE_FLAG_PROTOCOL.serialize(ofBytes(bytes), new RecordHeaders(), false);
    }

    private static byte[] createNonBackedText(final String text) {
        return serialize(serialize(text));
    }

    private static byte[] createBackedText(final String bucket, final String key) {
        final String uri = "foo://" + bucket + "/" + key;
        return serializeUri(uri);
    }

    private static byte[] createBackedText_(final String bucket, final String key) {
        final String uri = "foo://" + bucket + "/" + key;
        return LargeMessagePayload.getUriBytes(uri);
    }

    private static byte[] serialize(final String s) {
        return STRING_SERIALIZER.serialize(null, s);
    }

    private static Headers nonBackedHeaders(final boolean isKey) {
        return newHeaders(IS_NOT_BACKED, isKey);
    }

    private static Headers backedHeaders(final boolean isKey) {
        return newHeaders(IS_BACKED, isKey);
    }

    private static Headers newHeaders(final byte flag, final boolean isKey) {
        return new RecordHeaders()
                .add(getHeaderName(isKey), new byte[]{flag});
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldReadNonBackedText(final boolean isKey) {
        final LargeMessageRetrievingClient retriever = this.createRetriever();
        assertThat(retriever.retrieveBytes(createNonBackedText("foo"), new RecordHeaders(), isKey))
                .isEqualTo(serialize("foo"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldReadNonBackedTextWithHeaders(final boolean isKey) {
        final LargeMessageRetrievingClient retriever = this.createRetriever();
        final Headers headers = nonBackedHeaders(isKey);
        assertThat(retriever.retrieveBytes(serialize("foo"), headers, isKey))
                .isEqualTo(serialize("foo"));
        assertHasHeader(headers, isKey);
    }

    @ParameterizedTest
    @MethodSource("generateHeaders")
    void shouldReadNull(final Headers headers, final boolean isKey) {
        final LargeMessageRetrievingClient retriever = this.createRetriever();
        assertThat(retriever.retrieveBytes(null, headers, isKey))
                .isNull();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldReadBackedText(final boolean isKey) {
        final String bucket = "bucket";
        final String key = "key";
        when(this.client.getObject(bucket, key)).thenReturn(serialize("foo"));
        final LargeMessageRetrievingClient retriever = this.createRetriever();
        assertThat(retriever.retrieveBytes(createBackedText(bucket, key), new RecordHeaders(), isKey))
                .isEqualTo(serialize("foo"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldReadBackedTextWithHeaders(final boolean isKey) {
        final String bucket = "bucket";
        final String key = "key";
        when(this.client.getObject(bucket, key)).thenReturn(serialize("foo"));
        final LargeMessageRetrievingClient retriever = this.createRetriever();
        final Headers headers = backedHeaders(isKey);
        assertThat(retriever.retrieveBytes(createBackedText_(bucket, key), headers, isKey))
                .isEqualTo(serialize("foo"));
        assertHasHeader(headers, isKey);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldThrowExceptionOnErroneousFlag(final boolean isKey) {
        final LargeMessageRetrievingClient retriever = this.createRetriever();
        final Headers headers = new RecordHeaders();
        assertThatExceptionOfType(SerializationException.class)
                .isThrownBy(() -> retriever.retrieveBytes(new byte[]{2}, headers, isKey))
                .withMessage("Message can only be marked as backed or non-backed");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldThrowExceptionOnErroneousFlagWithHeaders(final boolean isKey) {
        final LargeMessageRetrievingClient retriever = this.createRetriever();
        final Headers headers = newHeaders((byte) 2, isKey);
        assertThatExceptionOfType(SerializationException.class)
                .isThrownBy(() -> retriever.retrieveBytes(new byte[]{}, headers, isKey))
                .withMessage("Message can only be marked as backed or non-backed");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldThrowExceptionOnErroneousUri(final boolean isKey) {
        final LargeMessageRetrievingClient retriever = this.createRetriever();
        final Headers headers = new RecordHeaders();
        assertThatExceptionOfType(SerializationException.class)
                .isThrownBy(() -> retriever.retrieveBytes(new byte[]{1, 0}, headers, isKey))
                .withCauseInstanceOf(URISyntaxException.class)
                .withMessage("Invalid URI");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldThrowExceptionOnErroneousUriWithHeaders(final boolean isKey) {
        final LargeMessageRetrievingClient retriever = this.createRetriever();
        final Headers headers = backedHeaders(isKey);
        assertThatExceptionOfType(SerializationException.class)
                .isThrownBy(() -> retriever.retrieveBytes(new byte[]{0}, headers, isKey))
                .withCauseInstanceOf(URISyntaxException.class)
                .withMessage("Invalid URI");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldThrowOnError(final boolean isKey) {
        final String bucket = "bucket";
        final String key = "key";
        when(this.client.getObject(bucket, key)).thenThrow(UncheckedIOException.class);
        final LargeMessageRetrievingClient retriever = this.createRetriever();
        final byte[] backedText = createBackedText(bucket, key);
        final Headers headers = new RecordHeaders();
        assertThatExceptionOfType(UncheckedIOException.class)
                .isThrownBy(() -> retriever.retrieveBytes(backedText, headers, isKey));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldThrowOnErrorWithHeaders(final boolean isKey) {
        final String bucket = "bucket";
        final String key = "key";
        when(this.client.getObject(bucket, key)).thenThrow(UncheckedIOException.class);
        final LargeMessageRetrievingClient retriever = this.createRetriever();
        final byte[] backedText = createBackedText_(bucket, key);
        final Headers headers = backedHeaders(isKey);
        assertThatExceptionOfType(UncheckedIOException.class)
                .isThrownBy(() -> retriever.retrieveBytes(backedText, headers, isKey));
    }

    private LargeMessageRetrievingClient createRetriever() {
        return new LargeMessageRetrievingClient(Collections.singletonMap("foo", () -> this.client));
    }

}
