package com.bakdata.kafka;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@UtilityClass
@Slf4j
class BlobStorageConfigLoader {
    static Map<String, Function<Map<?, ?>, BlobStorageConfig>> loadConfigFactories() {
        try (final ScanResult scanResult = new ClassGraph()
                .enableAnnotationInfo()
                .scan()) {
            final ClassInfoList classes = scanResult.getClassesWithAnnotation(BlobStorageType.class);
            final Map<String, Function<Map<?, ?>, BlobStorageConfig>> factories = classes.stream()
                    .map(BlobStorageConfigLoader::loadClass)
                    .collect(Collectors.toMap(BlobStorageConfigLoader::getScheme,
                            BlobStorageConfigLoader::asFactory));
            log.info("Found {} blob storage factories for types: {}", factories.size(), factories.keySet());
            return factories;
        }
    }

    private static Class<BlobStorageConfig> loadClass(final ClassInfo classInfo) {
        return classInfo.loadClass(BlobStorageConfig.class);
    }

    private static Function<Map<?, ?>, BlobStorageConfig> asFactory(final Class<? extends BlobStorageConfig> clazz) {
        return originals -> instantiate(clazz, originals);
    }

    private static String getScheme(final AnnotatedElement annotatedElement) {
        return annotatedElement.getAnnotation(BlobStorageType.class).value();
    }

    private static BlobStorageConfig instantiate(final Class<? extends BlobStorageConfig> clazz,
            final Map<?, ?> originals) {
        try {
            final Constructor<? extends BlobStorageConfig> constructor = clazz.getConstructor(Map.class);
            return constructor.newInstance(originals);
        } catch (final InstantiationException | IllegalAccessException | InvocationTargetException |
                       NoSuchMethodException e) {
            throw new IllegalStateException("Cannot instantiate blob storage config %s".formatted(clazz), e);
        }
    }
}
