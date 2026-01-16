package org.petmarket.logs;

import io.swagger.v3.oas.annotations.Operation;
import lombok.RequiredArgsConstructor;
import org.petmarket.utils.annotations.responses.ApiResponseForbidden;
import org.petmarket.utils.annotations.responses.ApiResponseSuccessful;
import org.petmarket.utils.annotations.responses.ApiResponseUnauthorized;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

@RestController
@RequiredArgsConstructor
@RequestMapping("/v1/admin/logs")
public class LogsController {
    @Value("${logging.file.path}")
    private String logFilePath;

    @Operation(summary = "Get logs")
    @ApiResponseSuccessful
    @ApiResponseUnauthorized
    @ApiResponseForbidden
    @GetMapping
    public ResponseEntity<List<String>> getLogs() {
        try {
            List<String> lines = Files.readAllLines(Paths.get(logFilePath));
            return ResponseEntity.ok(lines);
        } catch (IOException e) {
            return ResponseEntity.status(500).body(null);
        }
    }
}