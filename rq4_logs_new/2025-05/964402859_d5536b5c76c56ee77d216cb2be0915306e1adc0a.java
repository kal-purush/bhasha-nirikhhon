package com.fatepet.petrest.common;

import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.util.UUID;

@Service
public class S3Uploader {

    private final S3Client s3Client;
    private final String bucketName;
    private final Region region; // Region 필드를 추가
    private static final long MAX_FILE_SIZE = 5 * 1024 * 1024; // 5MB
    private static final String DEFAULT_PROFILE_IMAGE = "defaultProfile.png"; // 기본 프로필 이미지 파일명
    private static final String[] ALLOWED_CONTENT_TYPES = {"image/jpeg", "image/png", "image/gif"};

    public S3Uploader(S3Client s3Client, String bucketName, Region region) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.region = region; // Region을 생성자에서 전달받음
    }

    /**
     * 파일을 S3에 업로드하고 URL 반환
     *
     * @param file 업로드할 MultipartFile
     * @return 업로드된 파일의 URL
     */
    public String uploadFile(MultipartFile file) {
        // 파일 유효성 검사
        validateFile(file);

        String uniqueFileName = generateUniqueFileName(file.getOriginalFilename());

        try {
            // S3에 파일 업로드
            s3Client.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(uniqueFileName)
                            .contentType(file.getContentType())
                            .build(),
                    RequestBody.fromBytes(file.getBytes()) // 파일 데이터를 S3에 전송
            );

            // 업로드된 파일의 URL 생성 및 반환
            return generateFileUrl(uniqueFileName);

        } catch (Exception e) {
            throw new RuntimeException("파일 업로드 실패: " + e.getMessage(), e);
        }
    }

    /**
     * S3에서 파일 삭제
     *
     * @param profileImageUrl 삭제할 이미지 URL
     */
    public void deleteFile(String profileImageUrl) {
        if (profileImageUrl == null || profileImageUrl.isEmpty()) {
            return; // 삭제할 파일 없음
        }

        String fileName = extractFileNameFromUrl(profileImageUrl);

        // 기본 프로필 이미지는 삭제하지 않음
        if (DEFAULT_PROFILE_IMAGE.equals(fileName)) {
            return;
        }

        try {
            s3Client.deleteObject(
                    DeleteObjectRequest.builder()
                            .bucket(bucketName)
                            .key(fileName)
                            .build()
            );
        } catch (Exception e) {
            throw new RuntimeException("파일 삭제 실패: " + e.getMessage(), e);
        }
    }

    private String extractFileNameFromUrl(String profileImageUrl) {
        if (profileImageUrl == null || !profileImageUrl.contains("/")) {
            throw new IllegalArgumentException("잘못된 URL 형식입니다.");
        }
        return profileImageUrl.substring(profileImageUrl.lastIndexOf("/") + 1);
    }


    /**
     * 파일 유효성 검사 (크기 및 형식)
     *
     * @param file 업로드할 MultipartFile
     */
    private void validateFile(MultipartFile file) {

        // 파일 형식 검사
        boolean isValidContentType = false;
        for (String allowedContentType : ALLOWED_CONTENT_TYPES) {
            if (allowedContentType.equals(file.getContentType())) {
                isValidContentType = true;
                break;
            }
        }

        if (!isValidContentType) {
            throw new RuntimeException();
        }
    }

    /**
     * 고유한 파일 이름 생성
     *
     * @param originalFileName 원본 파일 이름
     * @return 고유한 파일 이름
     */
    private String generateUniqueFileName(String originalFileName) {
        String extension = originalFileName.substring(originalFileName.lastIndexOf("."));
        return UUID.randomUUID().toString() + extension;
    }

    /**
     * S3 객체의 URL 생성
     *
     * @param key S3에 저장된 파일의 키
     * @return 파일의 접근 URL
     */
    private String generateFileUrl(String key) {
        return String.format("https://%s.s3.%s.amazonaws.com/%s", bucketName, region.id(), key);
    }
}