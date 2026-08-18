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

import static software.amazon.awssdk.utils.StringUtils.isEmpty;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.utils.Utils;

/**
 * This class provides default configuration options for blob storage backed data. It offers configuration of the
 * following properties:
 * <ul>
 *     <li> maximum serialized message size in bytes
 *     <li> base path
 *     <li> id generator
 *     <li> usage of headers to store large message flag
 *     <li> acceptance of no headers as signal that message is not backed
 *     <li> compression type
 * </ul>
 */
@Slf4j
public class AbstractLargeMessageConfig extends AbstractConfig {
    public static final String PREFIX = "large.message.";
    public static final String MAX_BYTE_SIZE_CONFIG = PREFIX + "max.byte.size";
    public static final String MAX_BYTE_SIZE_DOC =
            "Maximum serialized message size in bytes before messages are stored on blob storage.";
    public static final int MAX_BYTE_SIZE_DEFAULT = 1000 * 1000;
    public static final String BASE_PATH_CONFIG = PREFIX + "base.path";
    public static final String BASE_PATH_DOC = "Base path to store data. Must include bucket and any prefix that "
            + "should be used, e.g., 's3://my-bucket/my/prefix/'. Available protocols: 's3', 'abs'.";
    public static final String BASE_PATH_DEFAULT = "";
    public static final String ID_GENERATOR_CONFIG = PREFIX + "id.generator";
    public static final String ID_GENERATOR_DOC = "Class to use for generating unique object IDs. Available "
            + "generators are: " + RandomUUIDGenerator.class.getName() + ", " + Sha256HashIdGenerator.class.getName()
            + ", " + MurmurHashIdGenerator.class.getName() + ".";
    public static final Class<? extends IdGenerator> ID_GENERATOR_DEFAULT = RandomUUIDGenerator.class;
    public static final String USE_HEADERS_CONFIG = PREFIX + "use.headers";
    public static final String USE_HEADERS_DOC =
            "Enable if Kafka message headers should be used to distinguish blob storage backed messages. This is "
                    + "disabled by default for backwards compatibility but leads to increased memory usage. It is "
                    + "recommended to enable this option.";
    public static final boolean USE_HEADERS_DEFAULT = false;
    public static final String ACCEPT_NO_HEADERS_CONFIG = PREFIX + "accept.no.headers";
    public static final String ACCEPT_NO_HEADERS_DOC =
            "Enable if messages read with no headers should be treated as non-backed messages. This allows enabling "
                    + "of large message behavior for data that has been serialized using the wrapped serializer.";
    public static final boolean ACCEPT_NO_HEADERS_DEFAULT = false;

    public static final String COMPRESSION_TYPE_CONFIG = PREFIX + "compression.type";
    public static final String COMPRESSION_TYPE_DOC =
            "The compression type for data stored in blob storage. The default is none (i.e. no compression). Valid "
                    + " values are <code>none</code>, <code>gzip</code>, <code>snappy</code>, <code>lz4</code>, or "
                    + "<code>zstd</code>. Note: this option is only available when kafka message headers are used.";
    public static final String COMPRESSION_TYPE_DEFAULT = "none";

    private static final ConfigDef config = baseConfigDef();
    private static final Map<String, Function<Map<?, ?>, BlobStorageConfig>> CLIENT_FACTORIES = new HashMap<>();
    private final Map<String, Supplier<BlobStorageClient>> clientFactories;

    /**
     * Create a new configuration from the given properties
     *
     * @param originals properties for configuring this config
     */
    public AbstractLargeMessageConfig(final Map<?, ?> originals) {
        super(config, originals);
        this.clientFactories = getClientFactories(originals);
    }

    protected AbstractLargeMessageConfig(final ConfigDef config, final Map<?, ?> originals) {
        super(config, originals);
        this.clientFactories = getClientFactories(originals);
    }

    public static void register(final String scheme, final Function<Map<?, ?>, BlobStorageConfig> factory) {
        CLIENT_FACTORIES.put(scheme, factory);
    }

    protected static ConfigDef baseConfigDef() {
        return new ConfigDef()
                .define(MAX_BYTE_SIZE_CONFIG, Type.INT, MAX_BYTE_SIZE_DEFAULT, Importance.MEDIUM, MAX_BYTE_SIZE_DOC)
                .define(BASE_PATH_CONFIG, Type.STRING, BASE_PATH_DEFAULT, Importance.HIGH, BASE_PATH_DOC)
                .define(USE_HEADERS_CONFIG, Type.BOOLEAN, USE_HEADERS_DEFAULT, Importance.MEDIUM, USE_HEADERS_DOC)
                .define(ACCEPT_NO_HEADERS_CONFIG, Type.BOOLEAN, ACCEPT_NO_HEADERS_DEFAULT, Importance.MEDIUM,
                        ACCEPT_NO_HEADERS_DOC)
                .define(ID_GENERATOR_CONFIG, Type.CLASS, ID_GENERATOR_DEFAULT, Importance.MEDIUM, ID_GENERATOR_DOC)
                .define(COMPRESSION_TYPE_CONFIG, Type.STRING, COMPRESSION_TYPE_DEFAULT, Importance.MEDIUM,
                        COMPRESSION_TYPE_DOC)
                ;
    }

    static SerializationException unknownScheme(final String scheme) {
        return new SerializationException("Unknown scheme for handling large messages: '" + scheme + "'");
    }

    private static NoBlobStorageClient createNoBlobStorageClient() {
        log.warn("No " + BASE_PATH_CONFIG + " has been provided and storing a large message will lead to an error.");
        return new NoBlobStorageClient();
    }

    protected static <T> T getInstance(final AbstractConfig config, final String key, final Class<T> targetClass) {
        final Class<?> configuredClass = config.getClass(key);
        if (configuredClass == null) {
            return null;
        }
        final Object o = Utils.newInstance(configuredClass);
        if (!targetClass.isInstance(o)) {
            throw new KafkaException(configuredClass.getName() + " is not an instance of " + targetClass.getName());
        }
        return targetClass.cast(o);
    }

    public LargeMessageStoringClient getStorer() {
        final BlobStorageClient client = this.getClient();
        return LargeMessageStoringClient.builder()
                .client(client)
                .basePath(this.getBasePath().orElse(null))
                .maxSize(this.getMaxSize())
                .idGenerator(this.getConfiguredInstance(ID_GENERATOR_CONFIG, IdGenerator.class))
                .protocol(this.getBoolean(USE_HEADERS_CONFIG) ? new HeaderLargeMessagePayloadProtocol()
                        : new ByteFlagLargeMessagePayloadProtocol())
                .compressionType(this.getCompressionType())
                .build();
    }

    private static Map<String, Supplier<BlobStorageClient>> getClientFactories(final Map<?, ?> originals) {
        return CLIENT_FACTORIES.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> {
                    final BlobStorageConfig config = e.getValue().apply(originals);
                    return config::createBlobStorageClient;
                }));
    }

    protected <T> T getInstance(final String key, final Class<T> targetClass) {
        return getInstance(this, key, targetClass);
    }

    public LargeMessageRetrievingClient getRetriever() {
        return new LargeMessageRetrievingClient(this.clientFactories, this.getBoolean(ACCEPT_NO_HEADERS_CONFIG));
    }

    private BlobStorageClient getClient() {
        return this.getBasePath()
                .map(BlobStorageURI::getScheme)
                .map(this::createClient)
                .orElseGet(AbstractLargeMessageConfig::createNoBlobStorageClient);
    }

    private BlobStorageClient createClient(final String scheme) {
        return Optional.ofNullable(this.clientFactories.get(scheme))
                .map(Supplier::get)
                .orElseThrow(() -> unknownScheme(scheme));
    }

    private Optional<BlobStorageURI> getBasePath() {
        final String basePath = this.getString(BASE_PATH_CONFIG);
        return isEmpty(basePath) ? Optional.empty() : Optional.of(BlobStorageURI.create(basePath));
    }

    private CompressionType getCompressionType() {
        return CompressionType.forName(this.getString(COMPRESSION_TYPE_CONFIG));
    }

    private int getMaxSize() {
        return this.getInt(MAX_BYTE_SIZE_CONFIG);
    }
}
