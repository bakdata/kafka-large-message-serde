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

import org.apache.kafka.common.header.Headers;

/**
 * Serialize and deserialize {@link LargeMessagePayload}
 */
public interface LargeMessagePayloadSerde {

    /**
     * Serialize a large message payload to bytes. Headers might be modified to store information for deserialization.
     *
     * @param payload large message payload
     * @param headers headers that might be used to store information for deserialization
     * @return bytes representing serialized payload
     */
    byte[] serialize(LargeMessagePayload payload, Headers headers);

    /**
     * Deserialize a large message payload from bytes and headers. Headers might be modified to remove deserialization
     * information.
     *
     * @param bytes serialized large message payload
     * @param headers headers that might be queried to retrieve information for deserialization
     * @return deserialized payload
     */
    LargeMessagePayload deserialize(byte[] bytes, Headers headers);
}
