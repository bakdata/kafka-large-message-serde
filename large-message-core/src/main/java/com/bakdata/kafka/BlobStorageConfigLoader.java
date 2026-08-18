package com.bakdata.kafka;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import java.util.Collection;
import java.util.List;
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

    private static List<BlobStorageConfigFactory> load(final Collection<? extends ClassInfo> classes) {
        final List<BlobStorageConfigFactory> factories = classes.stream()
                .map(BlobStorageConfigLoader::loadClass)
                .toList();
        final List<String> schemes = factories.stream()
                .map(BlobStorageConfigFactory::getScheme)
                .toList();
        log.info("Found {} blob storage factories for types: {}", factories.size(), schemes);
        return factories;
    }

    private static BlobStorageConfigFactory loadClass(final ClassInfo classInfo) {
        return new BlobStorageConfigFactory(classInfo.loadClass(BlobStorageConfig.class));
    }

}
