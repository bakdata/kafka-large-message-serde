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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import lombok.NonNull;
import lombok.Value;

/**
 * Class representing a large message. Contains info whether message is backed or not and the respective data.
 */
@Value
public class LargeMessagePayload {
    private static final Charset CHARSET = StandardCharsets.UTF_8;
    boolean backed;
    @NonNull byte[] data;

    static byte[] getUriBytes(final String uri) {
        return uri.getBytes(CHARSET);
    }

    static String asUri(final byte[] uriBytes) {
        return new String(uriBytes, CHARSET);
    }

    static LargeMessagePayload ofUri(final String uri) {
        final byte[] uriBytes = getUriBytes(uri);
        return new LargeMessagePayload(true, uriBytes);
    }

    static LargeMessagePayload ofBytes(final byte[] bytes) {
        return new LargeMessagePayload(false, bytes);
    }
}
