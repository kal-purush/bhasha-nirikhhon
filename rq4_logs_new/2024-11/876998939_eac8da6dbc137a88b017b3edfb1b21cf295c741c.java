package com.smartverse.churchlitebackend.services.s3;


import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;

import java.net.URL;
import java.time.Duration;

@Service
public class S3Service {

    private static final Region REGION = Region.US_EAST_2;
    private static final String BUCKET_NAME = "smartchurch";
    private final S3Presigner presigner;

    public S3Service() {

        AwsBasicCredentials awsCredentials = AwsBasicCredentials.create(System.getenv("AWS_ACCESS"), System.getenv("AWS_SECRET"));

        this.presigner = S3Presigner.builder()
                .region(REGION)
                .credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
                .build();
    }

    public URL requestUpload(String objectKey, int expirationInSeconds) {
        PresignedPutObjectRequest presignedRequest = presigner.presignPutObject(
                builder -> builder.signatureDuration(Duration.ofSeconds(expirationInSeconds))
                        .putObjectRequest(por -> por.bucket(BUCKET_NAME).key(objectKey))
        );
        return presignedRequest.url();
    }

    public URL requestDownload(String objectKey, int expirationInSeconds) {
        PresignedGetObjectRequest presignedRequest = presigner.presignGetObject(
                builder -> builder.signatureDuration(Duration.ofSeconds(expirationInSeconds))
                        .getObjectRequest(gor -> gor.bucket(BUCKET_NAME).key(objectKey))
        );
        return presignedRequest.url();
    }

    public void close() {
        presigner.close();
    }
}