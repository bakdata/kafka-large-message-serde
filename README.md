[![Build Status](https://dev.azure.com/bakdata/public/_apis/build/status/bakdata.kafka-s3-backed-serde?branchName=master)](https://dev.azure.com/bakdata/public/_build/latest?definitionId=20&branchName=master)
[![Sonarcloud status](https://sonarcloud.io/api/project_badges/measure?project=com.bakdata.kafka%3As3-backed-serde&metric=alert_status)](https://sonarcloud.io/dashboard?id=com.bakdata.kafka%3As3-backed-serde)
[![Code coverage](https://sonarcloud.io/api/project_badges/measure?project=com.bakdata.kafka%3As3-backed-serde&metric=coverage)](https://sonarcloud.io/dashboard?id=com.bakdata.kafka%3As3-backed-serde)
[![Maven](https://img.shields.io/maven-central/v/com.bakdata.kafka/s3-backed-serde.svg)](https://search.maven.org/search?q=g:com.bakdata.kafka%20AND%20a:s3-backed-serde&core=gav)

# kafka-s3-backed-serde
A Kafka Serde that reads and writes records from and to S3 transparently.

## Getting Started

You can add kafka-s3-backed-serde via Maven Central.

#### Gradle
```gradle
compile group: 'com.bakdata.kafka', name: 's3-backed-serde', version: '1.0.0'
```

#### Maven
```xml
<dependency>
    <groupId>com.bakdata.kafka</groupId>
    <artifactId>s3-backed-serde</artifactId>
    <version>1.0.0</version>
</dependency>
```


For other build tools or versions, refer to the [latest version in MvnRepository](https://mvnrepository.com/artifact/com.bakdata.kafka/s3-backed-serde/latest).

### Using the Serde

You can use it from your Kafka Streams application like any other Serde

```java
final Serde<String> serde = new S3BackedSerde<>();
serde.configure(Map.of(S3BackedSerdeConfig.BASE_PATH_CONFIG, "s3://my-bucket/",
        S3BackedSerdeConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class), false);
```

## Development

If you want to contribute to this project, you can simply clone the repository and build it via Gradle.
All dependencies should be included in the Gradle files, there are no external prerequisites.

```bash
> git clone git@github.com:bakdata/kafka-s3-backed-serde.git
> cd kafka-s3-backed-serde && ./gradlew build
```

Please note, that we have [code styles](https://github.com/bakdata/bakdata-code-styles) for Java.
They are basically the Google style guide, with some small modifications.

## Contributing

We are happy if you want to contribute to this project.
If you find any bugs or have suggestions for improvements, please open an issue.
We are also happy to accept your PRs.
Just open an issue beforehand and let us know what you want to do and why.

## License
This project is licensed under the MIT license.
Have a look at the [LICENSE](https://github.com/bakdata/kafka-s3-backed-serde/blob/master/LICENSE) for more details.
