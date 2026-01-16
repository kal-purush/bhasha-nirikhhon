package com.mydiary.my_diary_server.controller;

import com.google.gson.Gson;
import com.mydiary.my_diary_server.service.AudioRecongnitionService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@RequiredArgsConstructor
@RestController
@RequestMapping("/api/audio")
public class AudioRecognitionController {

    @Autowired
    AudioRecongnitionService audioRecongnitionService;

    @GetMapping
    public ResponseEntity<Void> test() throws IOException, InterruptedException {

        return ResponseEntity.ok().build();
    }

    @PostMapping("/upload")
    public ResponseEntity<String> handleFileUpload(@RequestPart MultipartFile audioFile) throws IOException, InterruptedException {
        if (audioFile.isEmpty()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Audio file is empty");
        }

        String msg = audioRecongnitionService.PostTranscribeSample(audioFile);

        return ResponseEntity.ok(msg);
    }

}