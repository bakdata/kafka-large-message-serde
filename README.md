[![Build Status](https://dev.azure.com/bakdata/public/_apis/build/status/bakdata.kafka-s3-backed-serde?branchName=master)](https://dev.azure.com/bakdata/public/_build/latest?definitionId=20&branchName=master)
[![Sonarcloud status](https://sonarcloud.io/api/project_badges/measure?project=com.bakdata.kafka%3As3-backed&metric=alert_status)](https://sonarcloud.io/dashboard?id=com.bakdata.kafka%3As3-backed)
[![Code coverage](https://sonarcloud.io/api/project_badges/measure?project=com.bakdata.kafka%3As3-backed&metric=coverage)](https://sonarcloud.io/dashboard?id=com.bakdata.kafka%3As3-backed)
[![Maven](https://img.shields.io/maven-central/v/com.bakdata.kafka/s3-backed-serde.svg)](https://search.maven.org/search?q=g:com.bakdata.kafka%20AND%20a:s3-backed-serde&core=gav)

# kafka-s3-backed-serde
A Kafka Serde that reads and writes records from and to S3 transparently.

## Getting Started

### Serde

You can add kafka-s3-backed-serde via Maven Central.

#### Gradle
```gradle
compile group: 'com.bakdata.kafka', name: 's3-backed-serde', version: '1.1.0'
```

#### Maven
```xml
<dependency>
    <groupId>com.bakdata.kafka</groupId>
    <artifactId>s3-backed-serde</artifactId>
    <version>1.1.0</version>
</dependency>
```

For other build tools or versions, refer to the [latest version in MvnRepository](https://mvnrepository.com/artifact/com.bakdata.kafka/s3-backed-serde/latest).

#### Usage

You can use it from your Kafka Streams application like any other Serde

```java
final Serde<String> serde = new S3BackedSerde<>();
serde.configure(Map.of(AbstractS3BackedConfig.BASE_PATH_CONFIG, "s3://my-bucket/",
        S3BackedSerdeConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class), false);
```

The following configuration options are available:

``s3backed.key.serde``
  Key serde class to use. All serde configurations are also delegated to this serde.

  * Type: class
  * Default: `org.apache.kafka.common.serialization.Serdes$ByteArraySerde`
  * Importance: high

``s3backed.value.serde``
  Value serde class to use. All serde configurations are also delegated to this serde.

  * Type: class
  * Default: `org.apache.kafka.common.serialization.Serdes$ByteArraySerde`
  * Importance: high

``s3backed.base.path``
  Base path to store data. Must include bucket and any prefix that should be used, e.g., `s3://my-bucket/my/prefix/`.

  * Type: string
  * Default: ""
  * Importance: high

``s3backed.max.byte.size``
  Maximum serialized message size in bytes before messages are stored on S3.

  * Type: int
  * Default: 1000000
  * Importance: medium

``s3backed.access.key``
  AWS access key to use for connecting to S3. Leave empty if AWS credential provider chain should be used.

  * Type: password
  * Default: ""
  * Importance: low

``s3backed.secret.key``
  AWS secret key to use for connecting to S3. Leave empty if AWS credential provider chain should be used.

  * Type: password
  * Default: ""
  * Importance: low

``s3backed.region``
  S3 region to use. Must be configured in conjunction with s3backed.endpoint. Leave empty if default S3 region should be used.

  * Type: string
  * Default: ""
  * Importance: low

``s3backed.endpoint``
  Endpoint to use for connection to Amazon S3. Must be configured in conjunction with s3backed.region. Leave empty if default S3 endpoint should be used.

  * Type: string
  * Default: ""
  * Importance: low

``s3backed.path.style.access``
  Enable path-style access for S3 client.

  * Type: boolean
  * Default: false
  * Importance: low

### Kafka Connect

This serde also comes with support for Kafka Connect.
You can add kafka-s3-backed-connect via Maven Central.

#### Gradle
```gradle
compile group: 'com.bakdata.kafka', name: 's3-backed-connect', version: '1.1.0'
```

#### Maven
```xml
<dependency>
    <groupId>com.bakdata.kafka</groupId>
    <artifactId>s3-backed-connect</artifactId>
    <version>1.1.0</version>
</dependency>
```

For other build tools or versions, refer to the [latest version in MvnRepository](https://mvnrepository.com/artifact/com.bakdata.kafka/s3-backed-connect/latest).

#### Usage

To use it with your Kafka Connect jobs, just configure your converter as `com.bakdata.kafka.S3BackedConverter`.

In addition to the configurations available for the serde (except `s3backed.key.serde` and `s3backed.value.serde`),
you can configure the following:

``s3backed.converter``
  Converter to use. All converter configurations are also delegated to this converter.

  * Type: class
  * Default: `org.apache.kafka.connect.converters.ByteArrayConverter`
  * Importance: high

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
