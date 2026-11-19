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

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@UtilityClass
@Slf4j
class BlobStorageConfigLoader {
    // Providers must be compatible with the BlobStorageConfigProvider loaded by this class loader. The context class
    // loader might not see the providers or might use a different copy of the interface, e.g., in Kafka Connect.
    static List<BlobStorageConfigProvider> loadProviders() {
        final ServiceLoader<BlobStorageConfigProvider> serviceLoader = ServiceLoader.load(
                BlobStorageConfigProvider.class, BlobStorageConfigProvider.class.getClassLoader());
        return load(serviceLoader.stream().map(Provider::get));
    }

    static List<BlobStorageConfigProvider> load(final Stream<BlobStorageConfigProvider> providers) {
        final List<BlobStorageConfigProvider> providerList = providers.toList();
        final Map<String, List<BlobStorageConfigProvider>> byScheme = providerList.stream()
                .collect(Collectors.groupingBy(BlobStorageConfigProvider::getScheme));
        final Map<String, List<String>> duplicates = byScheme.entrySet().stream()
                .filter(entry -> entry.getValue().size() > 1)
                .collect(Collectors.toMap(Entry::getKey, entry -> getClassNames(entry.getValue())));
        if (!duplicates.isEmpty()) {
            throw new IllegalStateException("Duplicate schemes found: %s".formatted(duplicates));
        }
        log.info("Found {} blob storage providers for types: {}", providerList.size(), byScheme.keySet());
        return providerList;
    }

    private static List<String> getClassNames(final List<? extends BlobStorageConfigProvider> providers) {
        return providers.stream()
                .map(provider -> provider.getClass().getName())
                .toList();
    }

}
