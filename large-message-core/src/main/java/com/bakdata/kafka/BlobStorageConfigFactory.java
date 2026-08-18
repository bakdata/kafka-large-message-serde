package com.bakdata.kafka;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Optional;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
class BlobStorageConfigFactory {
    private final @NonNull Class<? extends BlobStorageConfig> clazz;

    @Override
    public String toString() {
        return this.clazz.getName();
    }

    String getScheme() {
        return this.clazz.getAnnotation(BlobStorageType.class).value();
    }

    Optional<ConfigWithScheme> create(final Map<?, ?> originals) {
        try {
            final String scheme = this.getScheme();
            final BlobStorageConfig config = this.createConfig(originals);
            return Optional.of(new ConfigWithScheme(scheme, config));
        } catch (final InvocationTargetException | InstantiationException | IllegalAccessException |
                       NoSuchMethodException e) {
            log.error("Cannot create blob storage config {}", this.clazz.getName(), e);
            return Optional.empty();
        }
    }

    private BlobStorageConfig createConfig(final Map<?, ?> originals)
            throws InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException {
        final Constructor<? extends BlobStorageConfig> constructor = this.clazz.getConstructor(Map.class);
        return constructor.newInstance(originals);
    }
}
