/*
 * MIT License
 *
 * Copyright (c) 2025 bakdata
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
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.storage.Converter;

/**
 * This class provides configuration options for {@link LargeMessageConverter}. It offers configuration of the following
 * properties:
 * <p></p>
 * General
 * <ul>
 *     <li> converter
 *     <li> maximum serialized message size in bytes
 *     <li> base path
 *     <li> id generator
 * </ul>
 * <p></p>
 * Amazon S3 specific
 * <ul>
 *     <li> S3 endpoint
 *     <li> S3 region
 *     <li> S3 access key
 *     <li> S3 secret key
 *     <li> AWS security token service
 *     <li> S3 enable path-style access
 * </ul>
 * <p></p>
 * Azure Blob Storage specific
 * <ul>
 *     <li> Connection string
 * </ul>
 */
public class LargeMessageConverterConfig extends AbstractLargeMessageConfig {
    public static final String CONVERTER_CLASS_CONFIG = PREFIX + "converter";
    public static final Class<? extends Converter> CONVERTER_CLASS_DEFAULT = ByteArrayConverter.class;
    public static final String CONVERTER_CLASS_DOC =
            "Converter to use. All converter configurations are also delegated to this converter.";
    private static final ConfigDef config = configDef();

    protected LargeMessageConverterConfig(final Map<?, ?> originals) {
        super(config, originals);
    }

    private static ConfigDef configDef() {
        return baseConfigDef()
                .define(CONVERTER_CLASS_CONFIG, Type.CLASS, CONVERTER_CLASS_DEFAULT, Importance.HIGH,
                        CONVERTER_CLASS_DOC);
    }

    Converter getConverter() {
        return this.getInstance(CONVERTER_CLASS_CONFIG, Converter.class);
    }

}
