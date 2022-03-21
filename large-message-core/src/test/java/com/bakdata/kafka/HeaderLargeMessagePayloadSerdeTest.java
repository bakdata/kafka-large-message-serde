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
import static com.bakdata.kafka.HeaderLargeMessagePayloadSerde.HEADER;
import static com.bakdata.kafka.HeaderLargeMessagePayloadSerde.INSTANCE;
import static com.bakdata.kafka.HeaderLargeMessagePayloadSerde.usesHeaders;

import com.google.common.collect.Lists;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class HeaderLargeMessagePayloadSerdeTest {
    @InjectSoftAssertions
    private SoftAssertions softly;

    @Test
    void shouldSerializeBacked() {
        final byte[] payload = {2};
        final Headers headers = new RecordHeaders();
        this.softly.assertThat(INSTANCE.serialize(new LargeMessagePayload(true, payload), headers)).isEqualTo(payload);
        this.assertHasHeader(headers, IS_BACKED);
    }

    @Test
    void shouldSerializeNonBacked() {
        final byte[] payload = {2};
        final Headers headers = new RecordHeaders();
        this.softly.assertThat(INSTANCE.serialize(new LargeMessagePayload(false, payload), headers)).isEqualTo(payload);
        this.assertHasHeader(headers, IS_NOT_BACKED);
    }

    @Test
    void shouldReplaceHeader() {
        final byte[] payload = {2};
        final Headers headers = new RecordHeaders().add(HEADER, new byte[]{3});
        this.softly.assertThat(INSTANCE.serialize(new LargeMessagePayload(true, payload), headers)).isEqualTo(payload);
        this.assertHasHeader(headers, IS_BACKED);
    }

    @Test
    void shouldIgnoreOtherHeadersWhenSerializing() {
        final byte[] payload = {2};
        final Headers headers = new RecordHeaders().add("foo", new byte[]{3});
        this.softly.assertThat(INSTANCE.serialize(new LargeMessagePayload(true, payload), headers)).isEqualTo(payload);
        this.assertHasHeader(headers, IS_BACKED);
        this.softly.assertThat(headers.headers("foo")).hasSize(1);
    }

    @Test
    void shouldDeserializeBacked() {
        final byte[] payload = {2};
        final Headers headers = new RecordHeaders()
                .add(HEADER, new byte[]{IS_BACKED});
        this.softly.assertThat(INSTANCE.deserialize(payload, headers))
                .satisfies(largeMessagePayload -> {
                    this.softly.assertThat(largeMessagePayload.getData()).isEqualTo(payload);
                    this.softly.assertThat(largeMessagePayload.isBacked()).isTrue();
                });
        this.assertNoHeader(headers);
    }

    @Test
    void shouldDeserializeNonBacked() {
        final byte[] payload = {2};
        final Headers headers = new RecordHeaders()
                .add(HEADER, new byte[]{IS_NOT_BACKED});
        this.softly.assertThat(INSTANCE.deserialize(payload, headers))
                .satisfies(largeMessagePayload -> {
                    this.softly.assertThat(largeMessagePayload.getData()).isEqualTo(payload);
                    this.softly.assertThat(largeMessagePayload.isBacked()).isFalse();
                });
        this.assertNoHeader(headers);
    }

    @Test
    void shouldIgnoreOtherHeadersWhenDeserializing() {
        final byte[] payload = {2};
        final Headers headers = new RecordHeaders()
                .add("foo", new byte[]{1})
                .add(HEADER, new byte[]{IS_BACKED});
        this.softly.assertThat(INSTANCE.deserialize(payload, headers))
                .satisfies(largeMessagePayload -> {
                    this.softly.assertThat(largeMessagePayload.getData()).isEqualTo(payload);
                    this.softly.assertThat(largeMessagePayload.isBacked()).isTrue();
                });
        this.assertNoHeader(headers);
        this.softly.assertThat(headers.headers("foo")).hasSize(1);
    }

    @Test
    void shouldThrowExceptionOnErroneousFlag() {
        final byte[] payload = {2};
        final Headers headers = new RecordHeaders()
                .add(HEADER, new byte[]{2});
        this.softly.assertThatThrownBy(() -> INSTANCE.deserialize(payload, headers))
                .isInstanceOf(SerializationException.class)
                .hasMessage("Message can only be marked as backed or non-backed");
        this.assertNoHeader(headers);
    }

    @Test
    void shouldDetectHeaders() {
        this.softly.assertThat(usesHeaders(new RecordHeaders()
                        .add(HEADER, new byte[]{1})
                ))
                .isTrue();
        this.softly.assertThat(usesHeaders(new RecordHeaders()
                        .add(HEADER, new byte[]{2})
                        .add(HEADER, new byte[]{3})
                ))
                .isTrue();
        this.softly.assertThat(usesHeaders(new RecordHeaders())).isFalse();
    }

    private void assertNoHeader(final Headers headers) {
        this.softly.assertThat(headers.headers(HEADER)).isEmpty();
    }

    private void assertHasHeader(final Headers headers, final byte flag) {
        this.softly.assertThat(Lists.newArrayList(headers.headers(HEADER)))
                .hasSize(1)
                .anySatisfy(header -> this.softly.assertThat(header.value()).isEqualTo(new byte[]{flag}));
    }

}