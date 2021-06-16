package com.bakdata.kafka;

class MissingStorageClient implements BlobStorageClient {
    @Override
    public void deleteAllFiles(final String bucket, final String prefix) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String put(final byte[] bytes, final String bucket, final String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getObject(final String bucket, final String key) {
        throw new UnsupportedOperationException();
    }
}
