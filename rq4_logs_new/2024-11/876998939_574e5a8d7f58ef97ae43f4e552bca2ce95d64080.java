package com.smartverse.churchlitebackend.handlers.s3;


import com.smartverse.churchlitebackend_gen.RequestUpload;
import com.smartverse.churchlitebackend_gen.RequestUploadOutput;
import com.smartverse.churchlitebackend_gen.RequestUrl;
import com.smartverse.churchlitebackend_gen.RequestUrlOutput;
import org.springframework.http.ResponseEntity;

public class S3Impl implements RequestUpload, RequestUrl {


    @Override
    public ResponseEntity<RequestUploadOutput> RequestUpload(String fileType, String fileName) {
        return null;
    }

    @Override
    public ResponseEntity<RequestUrlOutput> RequestUrl(String fileName) {
        return null;
    }
}