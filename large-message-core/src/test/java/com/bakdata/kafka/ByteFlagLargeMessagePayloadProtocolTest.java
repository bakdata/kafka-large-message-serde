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

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(SoftAssertionsExtension.class)
class ByteFlagLargeMessagePayloadProtocolTest {
    @InjectSoftAssertions
    private SoftAssertions softly;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldSerializeBacked(final boolean isKey) {
        final byte[] payload = {2};
        final Headers headers = new RecordHeaders();
        this.softly.assertThat(
                        new ByteFlagLargeMessagePayloadProtocol().serialize(new LargeMessagePayload(true, payload),
                                headers, isKey))
                .isEqualTo(new byte[]{IS_BACKED, 2});
        this.assertNoHeaders(headers);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldSerializeNonBacked(final boolean isKey) {
        final byte[] payload = {2};
        final Headers headers = new RecordHeaders();
        this.softly.assertThat(
                        new ByteFlagLargeMessagePayloadProtocol().serialize(new LargeMessagePayload(false, payload),
                                headers, isKey))
                .isEqualTo(new byte[]{IS_NOT_BACKED, 2});
        this.assertNoHeaders(headers);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldIgnoreOtherHeadersWhenSerializing(final boolean isKey) {
        final byte[] payload = {2};
        final Headers headers = new RecordHeaders().add(getHeaderName(isKey), new byte[]{3});
        this.softly.assertThat(
                        new ByteFlagLargeMessagePayloadProtocol().serialize(new LargeMessagePayload(true, payload),
                                headers, isKey))
                .isEqualTo(new byte[]{IS_BACKED, 2});
        this.softly.assertThat(headers.headers(getHeaderName(isKey))).hasSize(1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldDeserializeBacked(final boolean isKey) {
        final byte[] payload = {IS_BACKED, 2};
        final Headers headers = new RecordHeaders();
        this.softly.assertThat(new ByteFlagLargeMessagePayloadProtocol().deserialize(payload, headers, isKey))
                .satisfies(largeMessagePayload -> {
                    this.softly.assertThat(largeMessagePayload.getData()).isEqualTo(new byte[]{2});
                    this.softly.assertThat(largeMessagePayload.isBacked()).isTrue();
                });
        this.assertNoHeaders(headers);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldDeserializeNonBacked(final boolean isKey) {
        final byte[] payload = {IS_NOT_BACKED, 2};
        final Headers headers = new RecordHeaders();
        this.softly.assertThat(new ByteFlagLargeMessagePayloadProtocol().deserialize(payload, headers, isKey))
                .satisfies(largeMessagePayload -> {
                    this.softly.assertThat(largeMessagePayload.getData()).isEqualTo(new byte[]{2});
                    this.softly.assertThat(largeMessagePayload.isBacked()).isFalse();
                });
        this.assertNoHeaders(headers);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldIgnoreOtherHeadersWhenDeserializing(final boolean isKey) {
        final byte[] payload = {IS_BACKED, 2};
        final Headers headers = new RecordHeaders()
                .add("foo", new byte[]{1});
        this.softly.assertThat(new ByteFlagLargeMessagePayloadProtocol().deserialize(payload, headers, isKey))
                .satisfies(largeMessagePayload -> {
                    this.softly.assertThat(largeMessagePayload.getData()).isEqualTo(new byte[]{2});
                    this.softly.assertThat(largeMessagePayload.isBacked()).isTrue();
                });
        this.softly.assertThat(headers.headers("foo")).hasSize(1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldThrowExceptionOnErroneousFlag(final boolean isKey) {
        final byte[] payload = {2, 2};
        final Headers headers = new RecordHeaders();
        this.softly.assertThatThrownBy(
                        () -> new ByteFlagLargeMessagePayloadProtocol().deserialize(payload, headers, isKey))
                .isInstanceOf(SerializationException.class)
                .hasMessage("Message can only be marked as backed or non-backed");
        this.assertNoHeaders(headers);
    }

    private void assertNoHeaders(final Iterable<Header> headers) {
        this.softly.assertThat(headers).isEmpty();
    }

}
