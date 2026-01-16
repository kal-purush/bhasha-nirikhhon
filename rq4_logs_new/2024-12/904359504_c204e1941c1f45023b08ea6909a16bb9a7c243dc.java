package com.example.pizza.service;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.stereotype.Service;
import org.springframework.util.FileSystemUtils;
import org.springframework.web.multipart.MultipartFile;

import jakarta.annotation.PostConstruct;

@Service
public class FileSystemStorageService {

    private final Path rootLocation;

    public FileSystemStorageService() {
        this.rootLocation = Paths.get("src\\main\\resources\\static\\uploads");
    }

    @PostConstruct
    public void init() {
        try {
            Files.createDirectories(rootLocation);
        } catch (IOException e) {
            throw new RuntimeException("Could not initialize folder for upload!");
        }
    }

    public String store(MultipartFile file) {
        try {
            // Lấy tên file gốc
            String originalFileName = file.getOriginalFilename();

            // Thay thế các ký tự đặc biệt bằng ký tự an toàn hơn (ví dụ, thay @ bằng _)
            String fileName = originalFileName.replaceAll("[^a-zA-Z0-9\\.\\-]", "_");

            Path destinationFile = this.rootLocation.resolve(fileName).normalize().toAbsolutePath();

            // Kiểm tra xem thư mục chứa file có phải là thư mục hiện tại hay không để tránh
            // lối trỏ thư mục bên ngoài
            if (!destinationFile.getParent().equals(this.rootLocation.toAbsolutePath())) {
                throw new RuntimeException("Cannot store file outside current directory.");
            }

            // Lưu file
            try (InputStream inputStream = file.getInputStream()) {
                Files.copy(inputStream, destinationFile, StandardCopyOption.REPLACE_EXISTING);
            }

            return fileName;
        } catch (IOException e) {
            throw new RuntimeException("Failed to store file", e);
        }
    }

}