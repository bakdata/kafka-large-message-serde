/*
 * MIT License
 *
 * Copyright (c) 2020 bakdata
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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class IdGeneratorTest {

    private static Stream<Arguments> generateIdGeneratorFactories() {
        return Stream.<Supplier<? extends IdGenerator>>of(Sha256HashIdGenerator::new, MurmurHashIdGenerator::new)
                .map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("generateIdGeneratorFactories")
    void shouldGenerateSameIdForSameInput(final Supplier<? extends IdGenerator> factory) {
        final String id = factory.get().generateId(new byte[]{(byte) 0});
        assertThat(factory.get().generateId(new byte[]{(byte) 0})).isEqualTo(id);
    }

    @ParameterizedTest
    @MethodSource("generateIdGeneratorFactories")
    void shouldGenerateDifferentIdForDifferentInput(final Supplier<? extends IdGenerator> factory) {
        final String id = factory.get().generateId(new byte[]{(byte) 0});
        assertThat(factory.get().generateId(new byte[]{(byte) 1})).isNotEqualTo(id);
    }

}