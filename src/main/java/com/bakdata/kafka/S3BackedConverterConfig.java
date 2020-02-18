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

import io.confluent.common.config.AbstractConfig;
import io.confluent.common.config.ConfigDef;
import io.confluent.common.config.ConfigDef.Importance;
import io.confluent.common.config.ConfigDef.Type;
import java.util.Map;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.storage.Converter;

public class S3BackedConverterConfig extends AbstractConfig {

    public static final String CONVERTER_CLASS_CONFIG = "inner.converter";
    public static final Class<? extends Converter> CONVERTER_CLASS_DEFAULT = ByteArrayConverter.class;
    public static final String CONVERTER_CLASS_DOC = "The converter type to use";

    private static final ConfigDef config = baseConfigDef();

    protected S3BackedConverterConfig(final Map<?, ?> originals) {
        super(config, originals);
    }

    private static ConfigDef baseConfigDef() {
        return new ConfigDef()
                .define(CONVERTER_CLASS_CONFIG, Type.CLASS, CONVERTER_CLASS_DEFAULT, Importance.MEDIUM,
                        CONVERTER_CLASS_DOC);
    }

    public Converter getConverter() {
        return this.getConfiguredInstance(CONVERTER_CLASS_CONFIG, Converter.class);
    }

}
