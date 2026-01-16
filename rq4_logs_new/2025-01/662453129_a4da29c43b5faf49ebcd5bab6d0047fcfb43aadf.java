package faang.school.postservice.controller;

import faang.school.postservice.service.FileService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RequiredArgsConstructor
@RequestMapping("/post")
@RestController
public class FileController {
    private final FileService fileService;

    @PostMapping("/image")
    public void addFile(@RequestBody String file) {
        fileService.addFile(file);
    }
}