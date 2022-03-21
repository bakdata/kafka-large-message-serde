/*
 * MIT License
 *
 * Copyright (c) 2021 bakdata
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
import static com.bakdata.kafka.HeaderLargeMessagePayloadSerde.HEADER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.when;

import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.stream.Stream;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class LargeMessageRetrievingClientTest {

    private static final Serializer<String> STRING_SERIALIZER = Serdes.String().serializer();
    @Mock
    BlobStorageClient client;

    static Stream<Arguments> generateHeaders() {
        return Stream.of(
                new RecordHeaders(),
                nonBackedHeaders()
        ).map(Arguments::of);
    }

    private static void assertNoHeader(final Headers headers) {
        assertThat(headers.headers(HEADER)).isEmpty();
    }

    private static byte[] serialize(final byte[] bytes) {
        return LargeMessageStoringClient.serialize(bytes, ByteArrayLargeMessagePayloadSerde.INSTANCE,
                new RecordHeaders());
    }

    private static byte[] createNonBackedText(final String text) {
        return serialize(serialize(text));
    }

    private static byte[] createBackedText(final String bucket, final String key) {
        final String uri = "foo://" + bucket + "/" + key;
        return LargeMessageStoringClientTest.serializeUri(uri);
    }

    private static byte[] createBackedText_(final String bucket, final String key) {
        final String uri = "foo://" + bucket + "/" + key;
        return LargeMessageStoringClient.getUriBytes(uri);
    }

    private static byte[] serialize(final String s) {
        return STRING_SERIALIZER.serialize(null, s);
    }

    private static Headers nonBackedHeaders() {
        return newHeaders(IS_NOT_BACKED);
    }

    private static Headers backedHeaders() {
        return newHeaders(IS_BACKED);
    }

    private static Headers newHeaders(final byte flag) {
        return new RecordHeaders()
                .add(HEADER, new byte[]{flag});
    }

    @Test
    void shouldReadNonBackedText() {
        final LargeMessageRetrievingClient retriever = this.createRetriever();
        assertThat(retriever.retrieveBytes(createNonBackedText("foo"), new RecordHeaders()))
                .isEqualTo(serialize("foo"));
    }

    @Test
    void shouldReadNonBackedTextWithHeaders() {
        final LargeMessageRetrievingClient retriever = this.createRetriever();
        final Headers headers = nonBackedHeaders();
        assertThat(retriever.retrieveBytes(serialize("foo"), headers))
                .isEqualTo(serialize("foo"));
        assertNoHeader(headers);
    }

    @ParameterizedTest
    @MethodSource("generateHeaders")
    void shouldReadNull(final Headers headers) {
        final LargeMessageRetrievingClient retriever = this.createRetriever();
        assertThat(retriever.retrieveBytes(null, headers))
                .isNull();
    }

    @Test
    void shouldReadBackedText() {
        final String bucket = "bucket";
        final String key = "key";
        when(this.client.getObject(bucket, key)).thenReturn(serialize("foo"));
        final LargeMessageRetrievingClient retriever = this.createRetriever();
        assertThat(retriever.retrieveBytes(createBackedText(bucket, key), new RecordHeaders()))
                .isEqualTo(serialize("foo"));
    }

    @Test
    void shouldReadBackedTextWithHeaders() {
        final String bucket = "bucket";
        final String key = "key";
        when(this.client.getObject(bucket, key)).thenReturn(serialize("foo"));
        final LargeMessageRetrievingClient retriever = this.createRetriever();
        final Headers headers = backedHeaders();
        assertThat(retriever.retrieveBytes(createBackedText_(bucket, key), headers))
                .isEqualTo(serialize("foo"));
        assertNoHeader(headers);
    }

    @Test
    void shouldThrowExceptionOnErroneousFlag() {
        final LargeMessageRetrievingClient retriever = this.createRetriever();
        final Headers headers = new RecordHeaders();
        assertThatExceptionOfType(SerializationException.class)
                .isThrownBy(() -> retriever.retrieveBytes(new byte[]{2}, headers))
                .withMessage("Message can only be marked as backed or non-backed");
    }

    @Test
    void shouldThrowExceptionOnErroneousFlagWithHeaders() {
        final LargeMessageRetrievingClient retriever = this.createRetriever();
        final Headers headers = newHeaders((byte) 2);
        assertThatExceptionOfType(SerializationException.class)
                .isThrownBy(() -> retriever.retrieveBytes(new byte[]{}, headers))
                .withMessage("Message can only be marked as backed or non-backed");
    }

    @Test
    void shouldThrowExceptionOnErroneousUri() {
        final LargeMessageRetrievingClient retriever = this.createRetriever();
        final Headers headers = new RecordHeaders();
        assertThatExceptionOfType(SerializationException.class)
                .isThrownBy(() -> retriever.retrieveBytes(new byte[]{1, 0}, headers))
                .withCauseInstanceOf(URISyntaxException.class)
                .withMessage("Invalid URI");
    }

    @Test
    void shouldThrowExceptionOnErroneousUriWithHeaders() {
        final LargeMessageRetrievingClient retriever = this.createRetriever();
        final Headers headers = backedHeaders();
        assertThatExceptionOfType(SerializationException.class)
                .isThrownBy(() -> retriever.retrieveBytes(new byte[]{0}, headers))
                .withCauseInstanceOf(URISyntaxException.class)
                .withMessage("Invalid URI");
    }

    @Test
    void shouldThrowOnError() {
        final String bucket = "bucket";
        final String key = "key";
        when(this.client.getObject(bucket, key)).thenThrow(UncheckedIOException.class);
        final LargeMessageRetrievingClient retriever = this.createRetriever();
        final byte[] backedText = createBackedText(bucket, key);
        final Headers headers = new RecordHeaders();
        assertThatExceptionOfType(UncheckedIOException.class)
                .isThrownBy(() -> retriever.retrieveBytes(backedText, headers));
    }

    @Test
    void shouldThrowOnErrorWithHeaders() {
        final String bucket = "bucket";
        final String key = "key";
        when(this.client.getObject(bucket, key)).thenThrow(UncheckedIOException.class);
        final LargeMessageRetrievingClient retriever = this.createRetriever();
        final byte[] backedText = createBackedText_(bucket, key);
        final Headers headers = backedHeaders();
        assertThatExceptionOfType(UncheckedIOException.class)
                .isThrownBy(() -> retriever.retrieveBytes(backedText, headers));
    }

    private LargeMessageRetrievingClient createRetriever() {
        return new LargeMessageRetrievingClient(Collections.singletonMap("foo", () -> this.client));
    }

}
