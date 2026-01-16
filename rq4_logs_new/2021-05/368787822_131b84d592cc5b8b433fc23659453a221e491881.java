package com.example.demo.controller;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import com.example.demo.lib.response.ResponseData;
import com.example.demo.service.upload.UploadService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("/upload")
public class UploadController {
  @Autowired
  private UploadService uploadService;

  @PostMapping
  public ResponseData<HashMap<String, List<String>>> uploadFiles(@RequestParam("files") List<MultipartFile> files)
      throws IOException {
    HashMap<String, List<String>> fileMap = uploadService.handleUploadFiles(files);
    return new ResponseData<HashMap<String, List<String>>>(HttpStatus.OK, "파일 업로드 성공", fileMap);
  }
}