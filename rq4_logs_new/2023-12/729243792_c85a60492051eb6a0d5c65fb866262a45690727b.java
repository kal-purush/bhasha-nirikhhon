package com.chwipoClova.common.utils;

import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;

public class FileUtil {
    public static String getOriginalFileExtension(MultipartFile file) throws IOException {
        try (InputStream inputStream = file.getInputStream()) {
            byte[] buffer = new byte[8]; // 파일의 시작 부분만 읽어올 수 있도록 8바이트로 설정
            int bytesRead = inputStream.read(buffer);

            if (bytesRead >= 0) {
                // 파일의 시작 부분에서 8바이트를 읽어온 후, 바이너리 데이터를 16진수 문자열로 변환하여 확인
                String fileSignature = bytesToHex(buffer, bytesRead);
                // 실제 파일 시그니처를 기준으로 확장자를 판별하거나 확장자 매핑을 확인할 수 있는 로직 구현
                return determineFileExtension(fileSignature);
            }
        }
        return "";
    }

    private static String bytesToHex(byte[] bytes, int bytesRead) {
        StringBuilder hex = new StringBuilder();
        for (int i = 0; i < bytesRead; i++) {
            hex.append(String.format("%02X", bytes[i]));
        }
        return hex.toString();
    }

    private static String determineFileExtension(String fileSignature) {
        // 여기에서 실제 파일 시그니처에 따라 확장자를 확인하거나 매핑하는 로직을 구현해야 합니다.
        // 예를 들어, 파일 시그니처를 분석하여 일치하는 확장자를 반환하거나 확장자 매핑을 통해 확인할 수 있습니다.
        // 파일 시그니처에 따라 정확한 확장자를 찾는 작업이 필요합니다.
        // 예시를 위해 파일 시그니처를 비교하는 대신 확장자를 하드코딩하여 반환합니다.

        // 여기에서 실제 파일 시그니처를 분석하여 확장자를 반환하는 로직을 추가해야 합니다.
        // 예시를 위해 간단히 특정 시그니처에 따라 확장자를 반환합니다.
        if (fileSignature.startsWith("FFD8FF") || fileSignature.startsWith("89504E47")) {
            return "image";
        } else if (fileSignature.startsWith("25504446")) {
            return "png";
        }

        // 해당되는 파일 시그니처를 찾지 못한 경우 null 반환
        return null;
    }
}