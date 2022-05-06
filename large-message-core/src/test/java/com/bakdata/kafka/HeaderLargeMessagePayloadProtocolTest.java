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
import static com.bakdata.kafka.HeaderLargeMessagePayloadProtocol.usesHeaders;

import com.google.common.collect.Lists;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(SoftAssertionsExtension.class)
class HeaderLargeMessagePayloadProtocolTest {
    @InjectSoftAssertions
    private SoftAssertions softly;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldSerializeBacked(final boolean isKey) {
        final byte[] payload = {2};
        final Headers headers = new RecordHeaders();
        final LargeMessagePayloadProtocol serde = new HeaderLargeMessagePayloadProtocol();
        this.softly.assertThat(serde.serialize(new LargeMessagePayload(true, payload), headers, isKey))
                .isEqualTo(payload);
        this.assertHasHeader(headers, IS_BACKED, isKey);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldSerializeNonBacked(final boolean isKey) {
        final byte[] payload = {2};
        final Headers headers = new RecordHeaders();
        final LargeMessagePayloadProtocol serde = new HeaderLargeMessagePayloadProtocol();
        this.softly.assertThat(serde.serialize(new LargeMessagePayload(false, payload), headers, isKey))
                .isEqualTo(payload);
        this.assertHasHeader(headers, IS_NOT_BACKED, isKey);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldReplaceHeader(final boolean isKey) {
        final byte[] payload = {2};
        final Headers headers = new RecordHeaders().add(getHeaderName(isKey), new byte[]{3});
        final LargeMessagePayloadProtocol serde = new HeaderLargeMessagePayloadProtocol();
        this.softly.assertThat(serde.serialize(new LargeMessagePayload(true, payload), headers, isKey))
                .isEqualTo(payload);
        this.assertHasHeader(headers, IS_BACKED, isKey);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldIgnoreOtherHeadersWhenSerializing(final boolean isKey) {
        final byte[] payload = {2};
        final Headers headers = new RecordHeaders().add("foo", new byte[]{3});
        final LargeMessagePayloadProtocol serde = new HeaderLargeMessagePayloadProtocol();
        this.softly.assertThat(serde.serialize(new LargeMessagePayload(true, payload), headers, isKey))
                .isEqualTo(payload);
        this.assertHasHeader(headers, IS_BACKED, isKey);
        this.softly.assertThat(headers.headers("foo")).hasSize(1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldDeserializeBacked(final boolean isKey) {
        final byte[] payload = {2};
        final Headers headers = new RecordHeaders()
                .add(getHeaderName(isKey), new byte[]{IS_BACKED});
        this.softly.assertThat(new HeaderLargeMessagePayloadProtocol().deserialize(payload, headers, isKey))
                .satisfies(largeMessagePayload -> {
                    this.softly.assertThat(largeMessagePayload.getData()).isEqualTo(payload);
                    this.softly.assertThat(largeMessagePayload.isBacked()).isTrue();
                });
        this.assertHasHeader(headers, IS_BACKED, isKey);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldDeserializeNonBacked(final boolean isKey) {
        final byte[] payload = {2};
        final Headers headers = new RecordHeaders()
                .add(getHeaderName(isKey), new byte[]{IS_NOT_BACKED});
        this.softly.assertThat(new HeaderLargeMessagePayloadProtocol().deserialize(payload, headers, isKey))
                .satisfies(largeMessagePayload -> {
                    this.softly.assertThat(largeMessagePayload.getData()).isEqualTo(payload);
                    this.softly.assertThat(largeMessagePayload.isBacked()).isFalse();
                });
        this.assertHasHeader(headers, IS_NOT_BACKED, isKey);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldIgnoreOtherHeadersWhenDeserializing(final boolean isKey) {
        final byte[] payload = {2};
        final Headers headers = new RecordHeaders()
                .add("foo", new byte[]{1})
                .add(getHeaderName(isKey), new byte[]{IS_BACKED});
        this.softly.assertThat(new HeaderLargeMessagePayloadProtocol().deserialize(payload, headers, isKey))
                .satisfies(largeMessagePayload -> {
                    this.softly.assertThat(largeMessagePayload.getData()).isEqualTo(payload);
                    this.softly.assertThat(largeMessagePayload.isBacked()).isTrue();
                });
        this.assertHasHeader(headers, IS_BACKED, isKey);
        this.softly.assertThat(headers.headers("foo")).hasSize(1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldThrowExceptionOnErroneousFlag(final boolean isKey) {
        final byte[] payload = {2};
        final Headers headers = new RecordHeaders()
                .add(getHeaderName(isKey), new byte[]{2});
        this.assertThatErroneousFlagExceptionIsThrownBy(
                () -> new HeaderLargeMessagePayloadProtocol().deserialize(payload, headers, isKey));
        this.assertHasHeader(headers, (byte) 2, isKey);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldThrowExceptionWhenCalledWithoutHeaders(final boolean isKey) {
        final byte[] payload = {2};
        this.softly.assertThatThrownBy(() -> new HeaderLargeMessagePayloadProtocol().deserialize(payload, isKey))
                .isInstanceOf(SerializationException.class)
                .hasMessage("Cannot deserialize without headers");
        this.softly.assertThatThrownBy(
                        () -> new HeaderLargeMessagePayloadProtocol().serialize(new LargeMessagePayload(true,
                                payload), isKey))
                .isInstanceOf(SerializationException.class)
                .hasMessage("Cannot serialize without headers");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldDetectHeaders(final boolean isKey) {
        this.softly.assertThat(usesHeaders(new RecordHeaders()
                        .add(getHeaderName(isKey), new byte[]{1}), isKey))
                .isTrue();
        this.softly.assertThat(usesHeaders(new RecordHeaders()
                        .add(getHeaderName(isKey), new byte[]{2})
                        .add(getHeaderName(isKey), new byte[]{3}), isKey))
                .isTrue();
        this.softly.assertThat(usesHeaders(new RecordHeaders(), isKey)).isFalse();
    }

    private void assertThatErroneousFlagExceptionIsThrownBy(final ThrowingCallable throwingCallable) {
        this.softly.assertThatThrownBy(throwingCallable)
                .isInstanceOf(SerializationException.class)
                .hasMessage("Message can only be marked as backed or non-backed");
    }

    private void assertHasHeader(final Headers headers, final byte flag, final boolean isKey) {
        this.softly.assertThat(Lists.newArrayList(headers.headers(getHeaderName(isKey))))
                .hasSize(1)
                .anySatisfy(header -> this.softly.assertThat(header.value()).isEqualTo(new byte[]{flag}));
    }

}
