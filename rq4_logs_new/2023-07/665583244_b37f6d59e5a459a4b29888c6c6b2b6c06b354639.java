package project.vilsoncake.BookOnlineStore.controller;

import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import project.vilsoncake.BookOnlineStore.entity.BookAvatarEntity;
import project.vilsoncake.BookOnlineStore.service.AvatarService;

import java.io.ByteArrayInputStream;

@RestController
@RequestMapping("/img")
public class BookAvatarController {

    private final AvatarService avatarService;

    public BookAvatarController(AvatarService avatarService) {
        this.avatarService = avatarService;
    }

    @GetMapping("/{id}")
    public ResponseEntity<InputStreamResource> getImage(@PathVariable("id") Long id) {
        BookAvatarEntity bookAvatar = avatarService.getAvatar(id);

        return ResponseEntity.ok()
                .header("fileName", bookAvatar.getFilename())
                .contentLength(bookAvatar.getSize())
                .body(new InputStreamResource(new ByteArrayInputStream(bookAvatar.getByteArray())));
    }
}