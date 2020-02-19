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

import io.confluent.common.config.ConfigDef;
import io.confluent.common.config.ConfigDef.Importance;
import io.confluent.common.config.ConfigDef.Type;
import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes.ByteArraySerde;

/**
 * This class provides configuration options for {@link S3BackedSerde}. It offers configuration of the following
 * properties:
 * <p>
 * <ul>
 *     <li> key serde class
 *     <li> value serde class
 *     <li> S3 endpoint
 *     <li> S3 region
 *     <li> S3 access key
 *     <li> S3 secret key
 *     <li> S3 enable path-style access
 *     <li> maximum message size
 *     <li> S3 base path
 * </ul>
 */
public class S3BackedSerdeConfig extends AbstractS3BackedConfig {
    public static final String KEY_SERDE_CLASS_CONFIG = PREFIX + "key.serde";
    public static final String KEY_SERDE_CLASS_DOC = "Key serde class to use.";
    public static final String VALUE_SERDE_CLASS_CONFIG = PREFIX + "value.serde";
    public static final Class<? extends Serde<?>> KEY_SERDE_CLASS_DEFAULT = ByteArraySerde.class;
    public static final String VALUE_SERDE_CLASS_DOC = "Value serde class to use.";
    public static final Class<? extends Serde<?>> VALUE_SERDE_CLASS_DEFAULT = ByteArraySerde.class;
    private static final ConfigDef config = configDef();

    S3BackedSerdeConfig(final Map<?, ?> originals) {
        super(config, originals);
    }

    private static ConfigDef configDef() {
        return baseConfigDef()
                .define(KEY_SERDE_CLASS_CONFIG, Type.CLASS, KEY_SERDE_CLASS_DEFAULT, Importance.HIGH,
                        KEY_SERDE_CLASS_DOC)
                .define(VALUE_SERDE_CLASS_CONFIG, Type.CLASS, VALUE_SERDE_CLASS_DEFAULT, Importance.HIGH,
                        VALUE_SERDE_CLASS_DOC);
    }

    <T> Serde<T> getKeySerde() {
        return this.getConfiguredInstance(KEY_SERDE_CLASS_CONFIG, Serde.class);
    }

    <T> Serde<T> getValueSerde() {
        return this.getConfiguredInstance(VALUE_SERDE_CLASS_CONFIG, Serde.class);
    }

}
