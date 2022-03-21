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

import static com.bakdata.kafka.FlagHelper.asFlag;
import static com.bakdata.kafka.FlagHelper.isBacked;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.header.Headers;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
final class ByteArrayLargeMessagePayloadSerde implements LargeMessagePayloadSerde {

    static final ByteArrayLargeMessagePayloadSerde INSTANCE = new ByteArrayLargeMessagePayloadSerde();

    static byte[] getBytes(final byte[] data) {
        final byte[] bytes = new byte[data.length - 1];
        // flag is stored in first byte
        System.arraycopy(data, 1, bytes, 0, data.length - 1);
        return bytes;
    }

    @Override
    public byte[] serialize(final LargeMessagePayload payload, final Headers headers) {
        final byte[] bytes = payload.getData();
        final byte[] fullBytes = new byte[bytes.length + 1];
        fullBytes[0] = asFlag(payload.isBacked());
        System.arraycopy(bytes, 0, fullBytes, 1, bytes.length);
        return fullBytes;
    }

    @Override
    public LargeMessagePayload deserialize(final byte[] data, final Headers headers) {
        return new LargeMessagePayload(isBacked(data[0]), getBytes(data));
    }
}
