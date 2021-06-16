package com.bakdata.kafka;

import java.net.URI;
import java.util.regex.Pattern;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class BlobStorageURI {
    private static final Pattern LEADING_SLASH = Pattern.compile("^/");
    private final @NonNull URI uri;

    public String getBucket() {
        return this.uri.getHost();
    }

    public String getKey() {
        return LEADING_SLASH.matcher(this.uri.getPath()).replaceAll("");
    }

    @Override
    public String toString() {
        return this.uri.toString();
    }
}
