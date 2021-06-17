package com.bakdata.kafka;

import java.net.URI;
import java.util.regex.Pattern;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
class BlobStorageURI {
    private static final Pattern LEADING_SLASH = Pattern.compile("^/");
    private final @NonNull URI uri;

    static BlobStorageURI create(final String rawUri) {
        final URI uri = URI.create(rawUri);
        return new BlobStorageURI(uri);
    }

    @Override
    public String toString() {
        return this.uri.toString();
    }

    String getScheme() {
        return this.uri.getScheme();
    }

    String getBucket() {
        return this.uri.getHost();
    }

    String getKey() {
        return LEADING_SLASH.matcher(this.uri.getPath()).replaceAll("");
    }
}
