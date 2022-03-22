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
import static com.bakdata.kafka.HeaderLargeMessagePayloadProtocol.HEADER;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class ByteFlagLargeMessagePayloadProtocolTest {
    @InjectSoftAssertions
    private SoftAssertions softly;

    @Test
    void shouldSerializeBacked() {
        final byte[] payload = {2};
        final Headers headers = new RecordHeaders();
        this.softly.assertThat(
                        new ByteFlagLargeMessagePayloadProtocol().serialize(new LargeMessagePayload(true, payload),
                                headers))
                .isEqualTo(new byte[]{IS_BACKED, 2});
        this.assertNoHeaders(headers);
    }

    @Test
    void shouldSerializeNonBacked() {
        final byte[] payload = {2};
        final Headers headers = new RecordHeaders();
        this.softly.assertThat(
                        new ByteFlagLargeMessagePayloadProtocol().serialize(new LargeMessagePayload(false, payload),
                                headers))
                .isEqualTo(new byte[]{IS_NOT_BACKED, 2});
        this.assertNoHeaders(headers);
    }

    @Test
    void shouldIgnoreOtherHeadersWhenSerializing() {
        final byte[] payload = {2};
        final Headers headers = new RecordHeaders().add(HEADER, new byte[]{3});
        this.softly.assertThat(
                        new ByteFlagLargeMessagePayloadProtocol().serialize(new LargeMessagePayload(true, payload),
                                headers))
                .isEqualTo(new byte[]{IS_BACKED, 2});
        this.softly.assertThat(headers.headers(HEADER)).hasSize(1);
    }

    @Test
    void shouldDeserializeBacked() {
        final byte[] payload = {IS_BACKED, 2};
        final Headers headers = new RecordHeaders();
        this.softly.assertThat(new ByteFlagLargeMessagePayloadProtocol().deserialize(payload, headers))
                .satisfies(largeMessagePayload -> {
                    this.softly.assertThat(largeMessagePayload.getData()).isEqualTo(new byte[]{2});
                    this.softly.assertThat(largeMessagePayload.isBacked()).isTrue();
                });
        this.assertNoHeaders(headers);
    }

    @Test
    void shouldDeserializeNonBacked() {
        final byte[] payload = {IS_NOT_BACKED, 2};
        final Headers headers = new RecordHeaders();
        this.softly.assertThat(new ByteFlagLargeMessagePayloadProtocol().deserialize(payload, headers))
                .satisfies(largeMessagePayload -> {
                    this.softly.assertThat(largeMessagePayload.getData()).isEqualTo(new byte[]{2});
                    this.softly.assertThat(largeMessagePayload.isBacked()).isFalse();
                });
        this.assertNoHeaders(headers);
    }

    @Test
    void shouldIgnoreOtherHeadersWhenDeserializing() {
        final byte[] payload = {IS_BACKED, 2};
        final Headers headers = new RecordHeaders()
                .add("foo", new byte[]{1});
        this.softly.assertThat(new ByteFlagLargeMessagePayloadProtocol().deserialize(payload, headers))
                .satisfies(largeMessagePayload -> {
                    this.softly.assertThat(largeMessagePayload.getData()).isEqualTo(new byte[]{2});
                    this.softly.assertThat(largeMessagePayload.isBacked()).isTrue();
                });
        this.softly.assertThat(headers.headers("foo")).hasSize(1);
    }

    @Test
    void shouldThrowExceptionOnErroneousFlag() {
        final byte[] payload = {2, 2};
        final Headers headers = new RecordHeaders();
        this.softly.assertThatThrownBy(() -> new ByteFlagLargeMessagePayloadProtocol().deserialize(payload, headers))
                .isInstanceOf(SerializationException.class)
                .hasMessage("Message can only be marked as backed or non-backed");
        this.assertNoHeaders(headers);
    }

    private void assertNoHeaders(final Headers headers) {
        this.softly.assertThat(headers).isEmpty();
    }

}
