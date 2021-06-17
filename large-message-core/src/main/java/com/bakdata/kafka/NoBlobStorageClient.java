package com.bakdata.kafka;

class NoBlobStorageClient implements BlobStorageClient {
    @Override
    public void deleteAllObjects(final String bucket, final String prefix) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String putObject(final byte[] bytes, final String bucket, final String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getObject(final String bucket, final String key) {
        throw new UnsupportedOperationException();
    }
}
