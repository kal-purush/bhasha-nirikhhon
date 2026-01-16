package gigjob.controller;

import gigjob.firebase.storage.FileStorageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequiredArgsConstructor
@Log4j2
public class ResourceController {
    private final FileStorageService fileStorageService;

    @PostMapping("/test/upload")
    public ResponseEntity create(@RequestParam(name = "file") MultipartFile file) {
        try {
            String fileName = fileStorageService.saveFile(file);
            String imageUrl = fileStorageService.getImageUrl(fileName);
            log.info("Upload " + fileName + " image url: " + imageUrl);
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage());
        }
        return ResponseEntity.ok().build();
    }
}