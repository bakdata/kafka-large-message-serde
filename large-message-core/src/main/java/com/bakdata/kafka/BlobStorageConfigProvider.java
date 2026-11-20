/*
 * MIT License
 *
 * Copyright (c) 2026 bakdata
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

import java.util.Map;

/**
 * Provides a {@link BlobStorageConfig} for a URI scheme. Providers are discovered using {@link java.util.ServiceLoader}
 * and used when storing and retrieving large messages using {@link AbstractLargeMessageConfig}. To register a provider,
 * list its fully qualified class name in {@code META-INF/services/com.bakdata.kafka.BlobStorageConfigProvider}.
 * Implementations must be public and have a public no-arg constructor.
 */
public interface BlobStorageConfigProvider {

    /**
     * Scheme handled by the {@link BlobStorageConfig} of this provider
     *
     * @return scheme
     */
    String getScheme();

    /**
     * Create a new {@link BlobStorageConfig}
     *
     * @param originals properties for configuring the config
     * @return blob storage config
     */
    BlobStorageConfig createConfig(Map<?, ?> originals);
}
