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

import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
class BlobStorageConfigLoaderTest {
    @InjectSoftAssertions
    private SoftAssertions softly;

    @Test
    void shouldLoad() {
        final Collection<BlobStorageConfigProvider> providers = BlobStorageConfigLoader.loadProviders();
        this.softly.assertThat(providers)
                .extracting(BlobStorageConfigProvider::getScheme)
                .containsExactlyInAnyOrder(TestBlobStorageConfig.SCHEME, InvalidBlobStorageConfigProvider.SCHEME);
    }

    @Test
    void shouldFailOnDuplicateScheme() {
        final Stream<BlobStorageConfigProvider> providers = Stream.of(
                new Duplicate1(),
                new Duplicate2()
        );
        this.softly.assertThatThrownBy(() -> BlobStorageConfigLoader.load(providers))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("Duplicate schemes found: {duplicate=[")
                .hasMessageContaining(Duplicate1.class.getName())
                .hasMessageContaining(Duplicate2.class.getName())
                .hasMessageEndingWith("]}");
    }

    private static class Duplicate1 implements BlobStorageConfigProvider {
        @Override
        public String getScheme() {
            return "duplicate";
        }

        @Override
        public BlobStorageConfig createConfig(final Map<?, ?> originals) {
            throw new UnsupportedOperationException();
        }
    }

    private static class Duplicate2 implements BlobStorageConfigProvider {
        @Override
        public String getScheme() {
            return "duplicate";
        }

        @Override
        public BlobStorageConfig createConfig(final Map<?, ?> originals) {
            throw new UnsupportedOperationException();
        }
    }

}
