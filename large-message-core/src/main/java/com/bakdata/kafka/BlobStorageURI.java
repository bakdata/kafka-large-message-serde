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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Pattern;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.errors.SerializationException;

@RequiredArgsConstructor
class BlobStorageURI {
    private static final Pattern LEADING_SLASH = Pattern.compile("^/");
    private final @NonNull URI uri;

    static BlobStorageURI create(final String rawUri) {
        try {
            final URI uri = new URI(rawUri);
            return new BlobStorageURI(uri);
        } catch (URISyntaxException e) {
            throw new SerializationException("Invalid URI", e);
        }
    }

    @Override
    public String toString() {
        return this.uri.toString();
    }

    String getScheme() {
        return this.uri.getScheme();
    }

    String getBucket() {
        return this.uri.getHost();
    }

    String getKey() {
        return LEADING_SLASH.matcher(this.uri.getPath()).replaceAll("");
    }
}
