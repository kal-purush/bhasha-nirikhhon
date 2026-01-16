package com.example.bootrestbookapplication.controllers;

import com.example.bootrestbookapplication.helper.FileUploadHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.multipart.MultipartFile;

@RestController
public class FileJUploadController {

    @Autowired
    private FileUploadHelper fileUploadHelper;

    @PostMapping("/upload-file")
    public ResponseEntity<String> uploadFile(@RequestParam("file") MultipartFile file) {

        try {
            if (file.isEmpty()) {
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Should must attach a file");
            }

            if (file.getContentType() == "image/jpeg") {
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Only JPEG or Images files are allowed");
            }

            System.out.println(file.getOriginalFilename());
            System.out.println(file.getSize());
            System.out.println(file.getName());

            boolean flag = fileUploadHelper.uploadFile(file);
            if(flag==true){
                return ResponseEntity.ok("File Got Uploaded");
            }

        } catch (Exception e) {
            e.printStackTrace();

        }
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Something went wrong. Please try again!");
    }
}