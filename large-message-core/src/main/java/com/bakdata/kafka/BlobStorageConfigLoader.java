package com.bakdata.kafka;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@UtilityClass
@Slf4j
class BlobStorageConfigLoader {
    static Collection<BlobStorageConfigFactory> loadConfigFactories() {
        try (final ScanResult scanResult = new ClassGraph()
                .enableAnnotationInfo()
                .scan()) {
            final ClassInfoList classes = scanResult.getClassesWithAnnotation(BlobStorageType.class);
            return load(classes);
        }
    }

    static List<BlobStorageConfigFactory> load(final Stream<Class<? extends BlobStorageConfig>> classes) {
        final List<BlobStorageConfigFactory> factories = classes
                .map(BlobStorageConfigFactory::new)
                .toList();
        final Map<String, List<BlobStorageConfigFactory>> byScheme = factories.stream()
                .collect(Collectors.groupingBy(BlobStorageConfigFactory::getScheme));
        final Map<String, List<BlobStorageConfigFactory>> duplicates = byScheme.entrySet().stream()
                .filter(entry -> entry.getValue().size() > 1)
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        if (!duplicates.isEmpty()) {
            throw new IllegalStateException("Duplicate schemes found: %s".formatted(duplicates));
        }
        log.info("Found {} blob storage factories for types: {}", factories.size(), byScheme.keySet());
        return factories;
    }

    private static List<BlobStorageConfigFactory> load(final Collection<? extends ClassInfo> classes) {
        final Stream<Class<? extends BlobStorageConfig>> classStream = classes.stream()
                .map(BlobStorageConfigLoader::loadClass);
        return load(classStream);
    }

    private static Class<? extends BlobStorageConfig> loadClass(final ClassInfo classInfo) {
        return classInfo.loadClass(BlobStorageConfig.class);
    }

}
