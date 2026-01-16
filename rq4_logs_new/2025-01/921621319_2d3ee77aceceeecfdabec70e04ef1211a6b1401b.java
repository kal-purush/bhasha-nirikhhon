package com.metamong.mt.global.file.local;

import java.io.IOException;
import java.io.InputStream;

import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.metamong.mt.global.apispec.BaseResponse;
import com.metamong.mt.global.file.FileUploader;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Profile("!prod")
@RestController
@RequestMapping("/api/files")
@RequiredArgsConstructor
@Slf4j
public class FileUploadController {
    private final FileUploader fileUploader;
    
    @PutMapping("/{*filePath}")
    public ResponseEntity<BaseResponse<Void>> putFile(
            @PathVariable("filePath") String filePath,
            InputStream is) throws IOException { // TODO: IOException을 어떻게 처리할 것인가
        if (log.isDebugEnabled()) {
            log.debug("filePath={}", filePath);
        }
        filePath = filePath.startsWith("/") ? filePath.substring(1) : filePath;
        this.fileUploader.storeFile(filePath, is);
        return ResponseEntity.ok(
                BaseResponse.of(HttpStatus.OK));
    }
}