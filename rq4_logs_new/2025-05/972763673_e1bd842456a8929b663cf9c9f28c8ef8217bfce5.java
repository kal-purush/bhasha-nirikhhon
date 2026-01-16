package com.github.classpick.file.upload.service;

import com.github.classpick.file.upload.dto.UploadImageResponse;
import com.github.classpick.global.external.aws.s3.S3Service;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class UploadService {

    private final S3Service s3Service;

    public UploadImageResponse uploadImage(String domain, Long targetId, String type) {

        String url = s3Service.generatePresignedUrl("%s/%d/proofs/%s/%s".formatted(
                domain,
                targetId,
                type,
                UUID.randomUUID()
        ));

        return UploadImageResponse.of(url);
    }
}