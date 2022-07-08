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

import static com.bakdata.kafka.ByteFlagLargeMessagePayloadProtocol.stripFlag;
import static com.bakdata.kafka.FlagHelper.IS_NOT_BACKED;
import static com.bakdata.kafka.HeaderLargeMessagePayloadProtocol.getHeaderName;
import static com.bakdata.kafka.LargeMessagePayload.getUriBytes;
import static com.bakdata.kafka.LargeMessageRetrievingClientTest.serializeUri;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.bakdata.kafka.LargeMessageStoringClient.LargeMessageStoringClientBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.UncheckedIOException;
import java.util.Map;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class LargeMessageStoringClientTest {

    private static final String TOPIC = "output";
    private static final Deserializer<String> STRING_DESERIALIZER = Serdes.String().deserializer();
    private static final Serializer<String> STRING_SERIALIZER = Serdes.String().serializer();
    @Mock
    private IdGenerator idGenerator;
    @Mock
    private BlobStorageClient client;
    @Mock
    private LargeMessagePayloadProtocol protocol;

    private static void expectNonBackedText(final String expected, final byte[] backedText) {
        assertThat(backedText[0]).isEqualTo(IS_NOT_BACKED);
        assertThat(STRING_DESERIALIZER.deserialize(null, stripFlag(backedText)))
                .isInstanceOf(String.class)
                .isEqualTo(expected);
    }

    private static void expectNonBackedText(final String expected, final byte[] backedText, final Headers headers,
            final boolean isKey) {
        assertHasHeader(headers, IS_NOT_BACKED, isKey);
        assertThat(STRING_DESERIALIZER.deserialize(null, backedText))
                .isInstanceOf(String.class)
                .isEqualTo(expected);
    }

    private static void assertHasHeader(final Headers headers, final byte flag, final boolean isKey) {
        assertThat(Lists.newArrayList(headers.headers(getHeaderName(isKey))))
                .hasSize(1)
                .anySatisfy(header -> assertThat(header.value()).isEqualTo(new byte[]{flag}));
    }

    private static byte[] serialize(final String s) {
        return STRING_SERIALIZER.serialize(null, s);
    }

    private static LargeMessageStoringClient createStorer(final Map<String, Object> properties) {
        final AbstractLargeMessageConfig config = new AbstractLargeMessageConfig(properties);
        return config.getStorer();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldWriteNonBackedText(final boolean isKey) {
        final LargeMessageStoringClient storer = this.createStorer()
                .maxSize(Integer.MAX_VALUE)
                .build();
        final Headers headers = new RecordHeaders();
        final byte[] fooBytes = serialize("foo");
        final byte[] returnBytes = {2};
        when(this.protocol.serialize(new LargeMessagePayload(false, fooBytes), headers, isKey)).thenReturn(returnBytes);
        assertThat(storer.storeBytes(null, fooBytes, isKey, headers)).isEqualTo(returnBytes);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldWriteNonBackedTextWithoutHeaders(final boolean isKey) {
        final LargeMessageStoringClient storer = this.createStorer()
                .maxSize(Integer.MAX_VALUE)
                .build();
        final byte[] fooBytes = serialize("foo");
        assertThat(storer.storeBytes(null, fooBytes, isKey))
                .isEqualTo(LargeMessageRetrievingClientTest.serialize(fooBytes));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldWriteNonBackedNull(final boolean isKey) {
        final LargeMessageStoringClient storer = this.createStorer()
                .maxSize(Integer.MAX_VALUE)
                .build();
        assertThat(storer.storeBytes(null, null, isKey, new RecordHeaders()))
                .isNull();
        verify(this.protocol, never()).serialize(any(), any(), eq(isKey));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldWriteNonBackedNullWithoutHeaders(final boolean isKey) {
        final LargeMessageStoringClient storer = this.createStorer()
                .maxSize(Integer.MAX_VALUE)
                .build();
        assertThat(storer.storeBytes(null, null, isKey))
                .isNull();
        verify(this.protocol, never()).serialize(any(), eq(isKey));
    }

    @Test
    void shouldWriteBackedTextKey() {
        final String bucket = "bucket";
        final String basePath = "foo://" + bucket + "/base/";
        when(this.idGenerator.generateId(serialize("foo"))).thenReturn("key");
        when(this.client.putObject(serialize("foo"), bucket, "base/" + TOPIC + "/keys/key"))
                .thenReturn("uri");
        final LargeMessageStoringClient storer = this.createStorer()
                .basePath(BlobStorageURI.create(basePath))
                .maxSize(0)
                .build();
        final Headers headers = new RecordHeaders();
        final byte[] uriBytes = getUriBytes("uri");
        final byte[] returnBytes = {2};
        when(this.protocol.serialize(new LargeMessagePayload(true, uriBytes), headers, true)).thenReturn(returnBytes);
        assertThat(storer.storeBytes(TOPIC, serialize("foo"), true, headers))
                .isEqualTo(returnBytes);
    }

    @Test
    void shouldWriteBackedTextKeyWithoutHeaders() {
        final String bucket = "bucket";
        final String basePath = "foo://" + bucket + "/base/";
        when(this.idGenerator.generateId(serialize("foo"))).thenReturn("key");
        when(this.client.putObject(serialize("foo"), bucket, "base/" + TOPIC + "/keys/key"))
                .thenReturn("uri");
        final LargeMessageStoringClient storer = this.createStorer()
                .basePath(BlobStorageURI.create(basePath))
                .maxSize(0)
                .build();
        assertThat(storer.storeBytes(TOPIC, serialize("foo"), true))
                .isEqualTo(serializeUri("uri"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldWriteBackedNull(final boolean isKey) {
        final LargeMessageStoringClient storer = this.createStorer()
                .maxSize(0)
                .build();
        assertThat(storer.storeBytes(null, null, isKey, new RecordHeaders()))
                .isNull();
        verify(this.protocol, never()).serialize(any(), any(), eq(isKey));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldWriteBackedNullWithoutHeaders(final boolean isKey) {
        final LargeMessageStoringClient storer = this.createStorer()
                .maxSize(0)
                .build();
        assertThat(storer.storeBytes(null, null, isKey))
                .isNull();
        verify(this.protocol, never()).serialize(any(), eq(isKey));
    }

    @Test
    void shouldWriteBackedTextValue() {
        final String bucket = "bucket";
        final String basePath = "foo://" + bucket + "/base/";
        when(this.idGenerator.generateId(serialize("foo"))).thenReturn("key");
        when(this.client.putObject(serialize("foo"), bucket, "base/" + TOPIC + "/values/key"))
                .thenReturn("uri");
        final LargeMessageStoringClient storer = this.createStorer()
                .basePath(BlobStorageURI.create(basePath))
                .maxSize(0)
                .build();
        final Headers headers = new RecordHeaders();
        final byte[] uriBytes = getUriBytes("uri");
        final byte[] returnBytes = {2};
        when(this.protocol.serialize(new LargeMessagePayload(true, uriBytes), headers, false)).thenReturn(returnBytes);
        assertThat(storer.storeBytes(TOPIC, serialize("foo"), false, headers))
                .isEqualTo(returnBytes);
    }

    @Test
    void shouldWriteBackedTextValueWithoutHeaders() {
        final String bucket = "bucket";
        final String basePath = "foo://" + bucket + "/base/";
        when(this.idGenerator.generateId(serialize("foo"))).thenReturn("key");
        when(this.client.putObject(serialize("foo"), bucket, "base/" + TOPIC + "/values/key"))
                .thenReturn("uri");
        final LargeMessageStoringClient storer = this.createStorer()
                .basePath(BlobStorageURI.create(basePath))
                .maxSize(0)
                .build();
        assertThat(storer.storeBytes(TOPIC, serialize("foo"), false))
                .isEqualTo(serializeUri("uri"));
    }

    @Test
    void shouldDeleteFiles() {
        final String bucket = "bucket";
        final String basePath = "foo://" + bucket + "/base/";
        final LargeMessageStoringClient storer = this.createStorer()
                .basePath(BlobStorageURI.create(basePath))
                .maxSize(0)
                .build();
        storer.deleteAllFiles(TOPIC);
        verify(this.client).deleteAllObjects(bucket, "base/" + TOPIC + "/");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldThrowExceptionOnError(final boolean isKey) {
        final String bucket = "bucket";
        final String basePath = "foo://" + bucket + "/base/";
        when(this.idGenerator.generateId(any())).thenReturn("key");
        when(this.client.putObject(any(), eq(bucket), any())).thenThrow(UncheckedIOException.class);
        final LargeMessageStoringClient storer = this.createStorer()
                .basePath(BlobStorageURI.create(basePath))
                .maxSize(0)
                .build();
        final byte[] foo = serialize("foo");
        final Headers headers = new RecordHeaders();
        assertThatExceptionOfType(UncheckedIOException.class)
                .isThrownBy(() -> storer.storeBytes(TOPIC, foo, isKey, headers));
        verify(this.protocol, never()).serialize(any(), any(), eq(isKey));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldThrowExceptionOnErrorWithoutHeaders(final boolean isKey) {
        final String bucket = "bucket";
        final String basePath = "foo://" + bucket + "/base/";
        when(this.idGenerator.generateId(any())).thenReturn("key");
        when(this.client.putObject(any(), eq(bucket), any())).thenThrow(UncheckedIOException.class);
        final LargeMessageStoringClient storer = this.createStorer()
                .basePath(BlobStorageURI.create(basePath))
                .maxSize(0)
                .build();
        final byte[] foo = serialize("foo");
        assertThatExceptionOfType(UncheckedIOException.class)
                .isThrownBy(() -> storer.storeBytes(TOPIC, foo, isKey));
        verify(this.protocol, never()).serialize(any(), eq(isKey));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldThrowExceptionOnNullTopic(final boolean isKey) {
        final String bucket = "bucket";
        final String basePath = "foo://" + bucket + "/base/";
        final LargeMessageStoringClient storer = this.createStorer()
                .basePath(BlobStorageURI.create(basePath))
                .maxSize(0)
                .build();
        final byte[] foo = serialize("foo");
        final Headers headers = new RecordHeaders();
        assertThatNullPointerException()
                .isThrownBy(() -> storer.storeBytes(null, foo, isKey, headers))
                .withMessage("Topic must not be null");
        verify(this.protocol, never()).serialize(any(), any(), eq(isKey));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldThrowExceptionOnNullTopicWithoutHeaders(final boolean isKey) {
        final String bucket = "bucket";
        final String basePath = "foo://" + bucket + "/base/";
        final LargeMessageStoringClient storer = this.createStorer()
                .basePath(BlobStorageURI.create(basePath))
                .maxSize(0)
                .build();
        final byte[] foo = serialize("foo");
        assertThatNullPointerException()
                .isThrownBy(() -> storer.storeBytes(null, foo, isKey))
                .withMessage("Topic must not be null");
        verify(this.protocol, never()).serialize(any(), eq(isKey));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldThrowExceptionOnNullBasePath(final boolean isKey) {
        final LargeMessageStoringClient storer = this.createStorer()
                .basePath(null)
                .maxSize(0)
                .build();
        final byte[] foo = serialize("foo");
        final Headers headers = new RecordHeaders();
        assertThatNullPointerException()
                .isThrownBy(() -> storer.storeBytes(TOPIC, foo, isKey, headers))
                .withMessage("Base path must not be null");
        verify(this.protocol, never()).serialize(any(), any(), eq(isKey));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldThrowExceptionOnNullBasePathWithoutHeaders(final boolean isKey) {
        final LargeMessageStoringClient storer = this.createStorer()
                .basePath(null)
                .maxSize(0)
                .build();
        final byte[] foo = serialize("foo");
        assertThatNullPointerException()
                .isThrownBy(() -> storer.storeBytes(TOPIC, foo, isKey))
                .withMessage("Base path must not be null");
        verify(this.protocol, never()).serialize(any(), eq(isKey));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldThrowExceptionOnNullIdGenerator(final boolean isKey) {
        final String bucket = "bucket";
        final String basePath = "foo://" + bucket + "/base/";
        final LargeMessageStoringClient storer = this.createStorer()
                .basePath(BlobStorageURI.create(basePath))
                .maxSize(0)
                .idGenerator(null)
                .build();
        final byte[] foo = serialize("foo");
        final Headers headers = new RecordHeaders();
        assertThatNullPointerException()
                .isThrownBy(() -> storer.storeBytes(TOPIC, foo, isKey, headers))
                .withMessage("Id generator must not be null");
        verify(this.protocol, never()).serialize(any(), any(), eq(isKey));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldThrowExceptionOnNullIdGeneratorWithoutHeaders(final boolean isKey) {
        final String bucket = "bucket";
        final String basePath = "foo://" + bucket + "/base/";
        final LargeMessageStoringClient storer = this.createStorer()
                .basePath(BlobStorageURI.create(basePath))
                .maxSize(0)
                .idGenerator(null)
                .build();
        final byte[] foo = serialize("foo");
        assertThatNullPointerException()
                .isThrownBy(() -> storer.storeBytes(TOPIC, foo, isKey))
                .withMessage("Id generator must not be null");
        verify(this.protocol, never()).serialize(any(), eq(isKey));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldWriteNonBackedTextWithConfig(final boolean isKey) {
        final Map<String, Object> properties = ImmutableMap.<String, Object>builder()
                .put(AbstractLargeMessageConfig.MAX_BYTE_SIZE_CONFIG, Integer.MAX_VALUE)
                .build();
        final LargeMessageStoringClient storer = createStorer(properties);
        assertThat(storer.storeBytes(null, serialize("foo"), isKey, new RecordHeaders()))
                .satisfies(backedText -> expectNonBackedText("foo", backedText));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldWriteNonBackedTextWithoutHeadersWithConfig(final boolean isKey) {
        final Map<String, Object> properties = ImmutableMap.<String, Object>builder()
                .put(AbstractLargeMessageConfig.MAX_BYTE_SIZE_CONFIG, Integer.MAX_VALUE)
                .build();
        final LargeMessageStoringClient storer = createStorer(properties);
        assertThat(storer.storeBytes(null, serialize("foo"), isKey))
                .satisfies(backedText -> expectNonBackedText("foo", backedText));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldWriteNonBackedTextWithConfigAndHeaders(final boolean isKey) {
        final Map<String, Object> properties = ImmutableMap.<String, Object>builder()
                .put(AbstractLargeMessageConfig.MAX_BYTE_SIZE_CONFIG, Integer.MAX_VALUE)
                .put(AbstractLargeMessageConfig.USE_HEADERS_CONFIG, true)
                .build();
        final LargeMessageStoringClient storer = createStorer(properties);
        final Headers headers = new RecordHeaders();
        assertThat(storer.storeBytes(null, serialize("foo"), isKey, headers))
                .satisfies(backedText -> expectNonBackedText("foo", backedText, headers, isKey));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldWriteNonBackedTextWithoutHeadersWithConfigAndHeaders(final boolean isKey) {
        final Map<String, Object> properties = ImmutableMap.<String, Object>builder()
                .put(AbstractLargeMessageConfig.MAX_BYTE_SIZE_CONFIG, Integer.MAX_VALUE)
                .put(AbstractLargeMessageConfig.USE_HEADERS_CONFIG, true)
                .build();
        final LargeMessageStoringClient storer = createStorer(properties);
        assertThat(storer.storeBytes(null, serialize("foo"), isKey))
                .satisfies(backedText -> expectNonBackedText("foo", backedText));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldWriteBackedTextWithConfig(final boolean isKey) {
        final Map<String, Object> properties = ImmutableMap.<String, Object>builder()
                .put(AbstractLargeMessageConfig.MAX_BYTE_SIZE_CONFIG, 0)
                .build();
        final LargeMessageStoringClient storer = createStorer(properties);
        final byte[] foo = serialize("foo");
        final Headers headers = new RecordHeaders();
        assertThatNullPointerException()
                .isThrownBy(() -> storer.storeBytes(TOPIC, foo, isKey, headers))
                .withMessage("Base path must not be null");
        verify(this.protocol, never()).serialize(any(), any(), eq(isKey));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldWriteBackedTextWithoutHeadersWithConfig(final boolean isKey) {
        final Map<String, Object> properties = ImmutableMap.<String, Object>builder()
                .put(AbstractLargeMessageConfig.MAX_BYTE_SIZE_CONFIG, 0)
                .build();
        final LargeMessageStoringClient storer = createStorer(properties);
        final byte[] foo = serialize("foo");
        assertThatNullPointerException()
                .isThrownBy(() -> storer.storeBytes(TOPIC, foo, isKey))
                .withMessage("Base path must not be null");
        verify(this.protocol, never()).serialize(any(), eq(isKey));
    }

    private LargeMessageStoringClientBuilder createStorer() {
        return LargeMessageStoringClient.builder()
                .idGenerator(this.idGenerator)
                .client(this.client)
                .protocol(this.protocol)
                .compressionType(CompressionType.NONE);
    }
}
