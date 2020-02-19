package com.bakdata.kafka;

import java.util.Map;
import lombok.NoArgsConstructor;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;

@NoArgsConstructor
public class S3BackedConverter implements Converter {
    private Converter converter;
    private S3StoringClient s3StoringClient;
    private S3RetrievingClient s3RetrievingClient;
    private boolean isKey;

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        final S3BackedConverterConfig config = new S3BackedConverterConfig(configs);
        this.s3StoringClient = config.getS3Storer();
        this.s3RetrievingClient = config.getS3Retriever();
        this.isKey = isKey;
        this.converter = config.getConverter();
        this.converter.configure(configs, isKey);
    }

    @Override
    public byte[] fromConnectData(final String topic, final Schema schema, final Object value) {
        final byte[] inner = this.converter.fromConnectData(topic, schema, value);
        return this.s3StoringClient.storeBytes(topic, inner, this.isKey);
    }

    @Override
    public SchemaAndValue toConnectData(final String topic, final byte[] value) {
        final byte[] inner = this.s3RetrievingClient.retrieveBytes(value);
        return this.converter.toConnectData(topic, inner);
    }

}
