package com.bakdata.kafka;

import lombok.NonNull;
import lombok.Value;

@Value
class ConfigWithScheme {
    @NonNull
    String scheme;
    @NonNull
    BlobStorageConfig config;
}
