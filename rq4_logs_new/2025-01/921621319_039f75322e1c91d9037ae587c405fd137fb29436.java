package com.metamong.mt.global.file.local;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
class LocalFileUploaderTest {
    private static final String TEST_FILENAME = "test-image.webp";
    
    private LocalFileUploader localFileUploader;
    private String fileSystemPath;
    
    @BeforeEach
    void beforeEach() {
        String fileSystemPath = getClass().getResource("/") + "static/";
        if (fileSystemPath.startsWith("file:/")) {
            fileSystemPath = fileSystemPath.substring("file:/".length());
        }
        log.info("filePath={}", fileSystemPath);
        this.fileSystemPath = fileSystemPath;
        this.localFileUploader = new LocalFileUploader(fileSystemPath);
        this.localFileUploader.init();
    }
    
    @AfterEach
    void afterEach() throws IOException {
//        File generatedFile = new File(this.fileSystemPath + TEST_FILENAME);
//        generatedFile.delete();
        
        File dirToRemove = new File(getClass().getResource("/static").toString().substring("file:/".length()));
        FileUtils.deleteDirectory(dirToRemove);
        log.info("dirToRemove={}", dirToRemove);
    }

    @Test
    @DisplayName("storeFile(String, InputStream) - 성공")
    void storeFile_InputStream_successful() throws IOException {
        // Given
        InputStream is = getClass().getResourceAsStream(TEST_FILENAME);
        
        // When
        this.localFileUploader.storeFile(TEST_FILENAME, is);
        
        // Then
        try (BufferedInputStream origBis = new BufferedInputStream(getClass().getResourceAsStream(TEST_FILENAME));
                BufferedInputStream generatedFileBis = new BufferedInputStream(new FileInputStream(new File(this.fileSystemPath + "resources/file/" + TEST_FILENAME)))) {
            int readSize1;
            int readSize2;
            final int bufferSize = 1024;
            byte[] buffer1 = new byte[bufferSize];
            byte[] buffer2 = new byte[bufferSize];
            
            while ((readSize1 = origBis.read(buffer1)) != -1
                    && (readSize2 = generatedFileBis.read(buffer2)) != -1) {
                assertThat(buffer2).isEqualTo(buffer1);
            }
        }
    }
}