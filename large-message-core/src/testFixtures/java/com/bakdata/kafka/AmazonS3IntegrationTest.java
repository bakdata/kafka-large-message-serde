/*
 * MIT License
 *
 * Copyright (c) 2025 bakdata
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.bakdata.kafka;

import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.util.Map;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

@Testcontainers
abstract class AmazonS3IntegrationTest {

    private static final DockerImageName LOCAL_STACK_IMAGE = DockerImageName.parse("localstack/localstack")
            .withTag("4.1.1");
    @Container
    private final LocalStackContainer localStackContainer = new LocalStackContainer(LOCAL_STACK_IMAGE)
            .withServices(Service.S3);

    S3Client getS3Client() {
        return S3Client.builder()
                .endpointOverride(this.getEndpointOverride())
                .credentialsProvider(StaticCredentialsProvider.create(this.getCredentials()))
                .region(this.getRegion())
                .build();
    }

    Map<String, String> getLargeMessageConfig() {
        final AwsBasicCredentials credentials = this.getCredentials();
        return ImmutableMap.<String, String>builder()
                .put(AbstractLargeMessageConfig.S3_ENDPOINT_CONFIG, this.getEndpointOverride().toString())
                .put(AbstractLargeMessageConfig.S3_REGION_CONFIG, this.getRegion().id())
                .put(AbstractLargeMessageConfig.S3_ACCESS_KEY_CONFIG, credentials.accessKeyId())
                .put(AbstractLargeMessageConfig.S3_SECRET_KEY_CONFIG, credentials.secretAccessKey())
                .build();
    }

    private Region getRegion() {
        return Region.of(this.localStackContainer.getRegion());
    }

    private AwsBasicCredentials getCredentials() {
        return AwsBasicCredentials.create(
                this.localStackContainer.getAccessKey(), this.localStackContainer.getSecretKey()
        );
    }

    private URI getEndpointOverride() {
        return this.localStackContainer.getEndpointOverride(Service.S3);
    }
}
