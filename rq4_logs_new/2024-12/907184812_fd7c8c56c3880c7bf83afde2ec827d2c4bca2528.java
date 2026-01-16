package tabom.myhands.domain.user.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import tabom.myhands.common.properties.S3Properties;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class S3ServiceImpl implements S3Service {

    private final S3Properties s3Properties;
    private final String bucketName = "myhands-bucket";

    @Override
    public String uploadFile(MultipartFile file) throws IOException {
        String uniqueFileName = "uploads/" + UUID.randomUUID() + "-" + file.getOriginalFilename();

        File tempFile = File.createTempFile("temp", null);
        file.transferTo(tempFile);

        S3Client s3Client = S3Client.builder()
                .region(Region.of(s3Properties.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(
                                s3Properties.getAccessKey(),
                                s3Properties.getSecretKey()
                        )
                ))
                .build();

        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(uniqueFileName)
                .contentType(file.getContentType())
                .build();

        s3Client.putObject(putObjectRequest, tempFile.toPath());
        Files.deleteIfExists(tempFile.toPath());

        return String.format("https://%s.s3.%s.amazonaws.com/%s", bucketName, s3Properties.getRegion(), uniqueFileName);
    }
}