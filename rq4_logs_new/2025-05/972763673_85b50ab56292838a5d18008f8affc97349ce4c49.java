package com.github.classpick.default_file.controller;


import com.github.classpick.default_file.controller.dto.GetDefaultFileInfoRes;
import com.github.classpick.default_file.controller.dto.SaveDefaultFileRes;
import com.github.classpick.default_file.repository.DefaultFileEntity;
import com.github.classpick.default_file.service.DefaultFileService;
import com.github.classpick.global.dto.Response;
import io.swagger.v3.oas.annotations.Hidden;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

@Hidden
@RequiredArgsConstructor
@RestController
public class DefaultFileController {

    private final DefaultFileService defaultFileService;

    @PostMapping("/v0.0/defaultfile")
    public Response<SaveDefaultFileRes> saveDefaultFile(
            @RequestParam("file") MultipartFile file,
            @RequestParam("userId") Long userId
    ) {

        defaultFileService.saveDefaultFile(file, userId);
        return Response.ok();
    }

    @GetMapping("/v0.0/file/{userID}")
    public Response<List<GetDefaultFileInfoRes>> getDefaultFile(@PathVariable Long userID) {

        List<DefaultFileEntity> fileList = defaultFileService.getDefaultFile(userID);
        return Response.ok(GetDefaultFileInfoRes.of(fileList));
    }

    @DeleteMapping("/v0.0/file/{fileId}")
    public Response<Void> deleteDefaultFile(@PathVariable Long fileId) {

        defaultFileService.deleteDefaultFile(fileId);
        return Response.ok();
    }
}