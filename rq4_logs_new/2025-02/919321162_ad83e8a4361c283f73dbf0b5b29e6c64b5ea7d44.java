package com.fptgang.backend.controller;

import com.fptgang.backend.service.impl.AzureBlobServiceImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/azure-blobs")
public class AzureBlobController {

    private final AzureBlobServiceImpl azureBlobService;

    @Value("${azure.storage.container-name}")
    private String containerName;

    public AzureBlobController(AzureBlobServiceImpl azureBlobService) {
        this.azureBlobService = azureBlobService;
    }

    /**
     * Endpoint to upload a file to Azure Blob Storage.
     *
     * @param base64Data Base64 encoded string of the file content
     * @param blobName The name of the blob (file name)
     * @return URL of the uploaded file in Azure Blob Storage
     */
    @PostMapping("/upload")
    public ResponseEntity<String> uploadBlob(@RequestParam("base64Data") String base64Data,
                                             @RequestParam("blobName") String blobName) {
        try {
            // Call AzureBlobService to upload the Base64 data to Azure Blob Storage
            String blobUrl = azureBlobService.upload(base64Data, blobName);

            // Return the blob URL
            return ResponseEntity.status(HttpStatus.CREATED).body("Blob uploaded successfully: " + blobUrl);
        } catch (Exception e) {
            // Return error if upload fails
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error uploading blob: " + e.getMessage());
        }
    }

    /**
     * Endpoint to get the status of the container and its blobs.
     *
     * @return Status of the container
     */
    @GetMapping("/status")
    public ResponseEntity<String> getStatus() {
        // Simple status endpoint to check if the service is up and the container is accessible
        return ResponseEntity.ok("Container: " + containerName + " is accessible.");
    }
}