package com.customs.network.fdapn.controller;

import com.customs.network.fdapn.model.PortCodeDetails;
import com.customs.network.fdapn.repository.PortCodeDetailsRepository;
import com.customs.network.fdapn.service.ExcelParser;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.List;
@RestController
public class PortCodeExcelController {
    private final PortCodeDetailsRepository repository;

    public PortCodeExcelController(PortCodeDetailsRepository repository) {
        this.repository = repository;
    }

    @PostMapping("/upload")
    public ResponseEntity<String> uploadExcel(@RequestParam("file") MultipartFile file) throws IOException {
        if (file.isEmpty()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please upload a file.");
        }
        List<PortCodeDetails> portCodeDetailsList = ExcelParser.parse(file);
        repository.saveAll(portCodeDetailsList);
        return ResponseEntity.ok("Data uploaded successfully.");
    }
}