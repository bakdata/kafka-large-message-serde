[![Build Status](https://dev.azure.com/bakdata/public/_apis/build/status/bakdata.kafka-large-message-serde?branchName=master)](https://dev.azure.com/bakdata/public/_build/latest?definitionId=20&branchName=master)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=com.bakdata.kafka%3Alarge-message&metric=alert_status)](https://sonarcloud.io/dashboard?id=com.bakdata.kafka%3Alarge-message)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=com.bakdata.kafka%3Alarge-message&metric=coverage)](https://sonarcloud.io/dashboard?id=com.bakdata.kafka%3Alarge-message)
[![Maven](https://img.shields.io/maven-central/v/com.bakdata.kafka/large-message-serde.svg)](https://search.maven.org/search?q=g:com.bakdata.kafka%20AND%20a:large-message-serde&core=gav)

# kafka-large-message-serde
A Kafka Serde that reads and writes records from and to a blob storage, such as Amazon S3, Azure Blob Storage, and Google Cloud Storage, transparently.
Formerly known as kafka-s3-backed-serde.

## Getting Started

### Serde

You can add kafka-large-message-serde via Maven Central.

#### Gradle
```gradle
implementation group: 'com.bakdata.kafka', name: 'large-message-serde', version: '2.0.0'
```

#### Maven
```xml
<dependency>
    <groupId>com.bakdata.kafka</groupId>
    <artifactId>large-message-serde</artifactId>
    <version>2.0.0</version>
</dependency>
```

For other build tools or versions, refer to the [latest version in MvnRepository](https://mvnrepository.com/artifact/com.bakdata.kafka/large-message-serde/latest).

Make sure to also add [Confluent Maven Repository](http://packages.confluent.io/maven/) to your build file.

#### Usage

You can use it from your Kafka Streams application like any other Serde

```java
final Serde<String> serde = new LargeMessageSerde<>();
serde.configure(Map.of(AbstractLargeMessageConfig.BASE_PATH_CONFIG, "s3://my-bucket/",
        LargeMessageSerdeConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class), false);
```

The following configuration options are available:

``large.message.key.serde``
  Key serde class to use. All serde configurations are also delegated to this serde.

  * Type: class
  * Default: `org.apache.kafka.common.serialization.Serdes$ByteArraySerde`
  * Importance: high

``large.message.value.serde``
  Value serde class to use. All serde configurations are also delegated to this serde.

  * Type: class
  * Default: `org.apache.kafka.common.serialization.Serdes$ByteArraySerde`
  * Importance: high

``large.message.base.path``
  Base path to store data. Must include bucket and any prefix that should be used, e.g., `s3://my-bucket/my/prefix/`. Available protocols: `s3`, `abs`.

  * Type: string
  * Default: ""
  * Importance: high

``large.message.max.byte.size``
  Maximum serialized message size in bytes before messages are stored on blob storage.

  * Type: int
  * Default: 1000000
  * Importance: medium
  
``large.message.use.headers``
Enable if Kafka message headers should be used to distinguish blob storage backed messages. This is disabled by default
for backwards compatibility but leads to increased memory usage. It is recommended to enable this option.

  * Type: boolean
  * Default: false
  * Importance: medium
  
``large.message.id.generator``
  Class to use for generating unique object IDs. Available generators are: `com.bakdata.kafka.RandomUUIDGenerator`, `com.bakdata.kafka.Sha256HashIdGenerator`, `com.bakdata.kafka.MurmurHashIdGenerator`.

  * Type: class
  * Default: `com.bakdata.kafka.RandomUUIDGenerator`
  * Importance: medium

``large.message.s3.access.key``
  AWS access key to use for connecting to S3. Leave empty if AWS credential provider chain or STS Assume Role provider should be used.

  * Type: password
  * Default: ""
  * Importance: low

``large.message.s3.secret.key``
  AWS secret key to use for connecting to S3. Leave empty if AWS credential provider chain or STS Assume Role provider should be used.

  * Type: password
  * Default: ""
  * Importance: low

 ``large.message.s3.sts.role.arn``
   AWS STS role ARN to use for connecting to S3. Leave empty if AWS Basic provider or AWS credential provider chain should be used.

   * Type: string
   * Default: ""
   * Importance: low

  
 ``large.message.s3.role.external.id``
   AWS STS role external ID used when retrieving session credentials under an assumed role. Leave empty if AWS Basic provider or AWS credential provider chain should be used.

   * Type: string
   * Default: ""
   * Importance: low

 ``large.message.s3.role.session.name``
   AWS STS role session name to use when starting a session. Leave empty if AWS Basic provider or AWS credential provider chain should be used.

   * Type: string
   * Default: ""
   * Importance: low

``large.message.s3.jwt.path``
   Path to an OIDC token file in JSON format (JWT) used to authenticate before AWS STS role authorisation, e.g. for EKS `/var/run/secrets/eks.amazonaws.com/serviceaccount/token`.

   * Type: string
   * Default: ""
   * Importance: low

``large.message.s3.region``
S3 region to use. Leave empty if default S3 region should be used.

  * Type: string
  * Default: ""
  * Importance: low

``large.message.s3.endpoint``
Endpoint to use for connection to Amazon S3. Leave empty if default S3 endpoint should be used.

  * Type: string
  * Default: ""
  * Importance: low

``large.message.abs.connection.string``
  Azure connection string for connection to blob storage. Leave empty if Azure credential provider chain should be used.

  * Type: password
  * Default: ""
  * Importance: low

``large.message.gs.key.path``
  Google service account key JSON path. Leave empty If the environment variable GOOGLE_APPLICATION_CREDENTIALS is set
  or if you want to use the default service account provided by your computer engine. For more information about
  authenticating as a service account please 
  refer to the [main documentation](https://cloud.google.com/docs/authentication/production).

  * Type: string
  * Default: ""
  * Importance: low

``large.message.compression.type``
  The compression type for data stored in blob storage. The default is `none` (i.e. no compression). Valid values are `none`,
  `gzip`, `snappy`, `lz4` and `zstd`.
  Note: this option is only available when `large.message.use.headers` is enabled.

  * Type: string
  * Default: "none"
  * Importance: low


### Kafka Connect

This serde also comes with support for Kafka Connect.
You can add kafka-large-message-connect via Maven Central.

#### Gradle
```gradle
implementation group: 'com.bakdata.kafka', name: 'large-message-connect', version: '1.1.6'
```

#### Maven
```xml
<dependency>
    <groupId>com.bakdata.kafka</groupId>
    <artifactId>large-message-connect</artifactId>
    <version>2.0.0</version>
</dependency>
```

For other build tools or versions, refer to the [latest version in MvnRepository](https://mvnrepository.com/artifact/com.bakdata.kafka/large-message-connect/latest).

#### Usage

To use it with your Kafka Connect jobs, just configure your converter as `com.bakdata.kafka.LargeMessageConverter`.

In addition to the configurations available for the serde (except `large.message.key.serde` and `large.message.value.serde`),
you can configure the following:

``large.message.converter``
  Converter to use. All converter configurations are also delegated to this converter.

  * Type: class
  * Default: `org.apache.kafka.connect.converters.ByteArrayConverter`
  * Importance: high

For general guidance on how to configure Kafka Connect converters, please have a look at the [official documentation](https://docs.confluent.io/home/connect/configuring.html).

### Cleaning up the bucket

We also provide a method for cleaning up all files on the blob storage associated with a topic:

```java
final Map<String, Object> properties = ...;
final AbstractLargeMessageConfig config = new AbstractLargeMessageConfig(properties);
final LargeMessageStoringClient storer = config.getStorer()
storer.deleteAllFiles("topic");
```

## Development

If you want to contribute to this project, you can simply clone the repository and build it via Gradle.
All dependencies should be included in the Gradle files, there are no external prerequisites.

```bash
> git clone git@github.com:bakdata/kafka-large-message-serde.git
> cd kafka-large-message-serde && ./gradlew build
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
Have a look at the [LICENSE](https://github.com/bakdata/kafka-large-message-serde/blob/master/LICENSE) for more details.
