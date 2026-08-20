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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Optional;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
class BlobStorageConfigFactory {
    private final @NonNull Class<? extends BlobStorageConfig> clazz;

    @Override
    public String toString() {
        return this.clazz.getName();
    }

    String getScheme() {
        return this.clazz.getAnnotation(BlobStorageType.class).value();
    }

    Optional<ConfigWithScheme> create(final Map<?, ?> originals) {
        try {
            final String scheme = this.getScheme();
            final BlobStorageConfig config = this.createConfig(originals);
            return Optional.of(new ConfigWithScheme(scheme, config));
        } catch (final InvocationTargetException | InstantiationException | IllegalAccessException |
                       NoSuchMethodException e) {
            log.error("Cannot create blob storage config {}", this.clazz.getName(), e);
            return Optional.empty();
        }
    }

    private BlobStorageConfig createConfig(final Map<?, ?> originals)
            throws InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException {
        final Constructor<? extends BlobStorageConfig> constructor = this.clazz.getConstructor(Map.class);
        return constructor.newInstance(originals);
    }
}
