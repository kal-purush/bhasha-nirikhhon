package com.damda.global.image;

import com.damda.global.exception.StorageException;
import org.apache.tika.Tika;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;

@Component
public class ImageValidator {

    private final Tika tika = new Tika();

    public void validateImageFile(MultipartFile multipartFile) {
        if (multipartFile.isEmpty()) {
            throw new IllegalArgumentException("이미지가 존재하지 않습니다.");
        }
        if (!isImageFile(multipartFile)) {
            throw new IllegalArgumentException("이미지 파일이 아닙니다.");
        }
    }

    // 파일명의 확장자는 위변조 가능. MIME Type으로 비교
    private boolean isImageFile(MultipartFile multipartFile) {
        try {
            String mimeType = tika.detect(multipartFile.getInputStream());
            if (!mimeType.startsWith("image")) {
                return false;
            }
        } catch (IOException e) {
            throw new StorageException("이미지 저장에 실패하였습니다.");
        }
        return true;
    }
}