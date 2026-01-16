package com.ssu.commerce.book.controller;

import com.ssu.commerce.book.dto.mapper.RegisterImageResponseDtoMapper;
import com.ssu.commerce.book.dto.mapper.RegisterImageParamDtoMapper;
import com.ssu.commerce.book.dto.request.RegisterImageRequestDto;
import com.ssu.commerce.book.dto.response.RegisterImageResponseDto;
import com.ssu.commerce.book.file.ImageStore;
import com.ssu.commerce.book.service.ImageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.List;
import java.util.UUID;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/v1/image")
public class ImageController {

    @Autowired
    private ImageStore imageStore;

    private final ImageService imageService;

    @PostMapping(value = "", consumes = {MediaType.APPLICATION_JSON_VALUE, MediaType.MULTIPART_FORM_DATA_VALUE})
    public List<RegisterImageResponseDto> registerImage(
            @RequestPart final RegisterImageRequestDto registerImageRequestDto,
            @RequestPart @NotNull final List<MultipartFile> fileList
    ) throws IOException {

        log.debug("[registerImage]bookId : {}, fileListSize : {}", registerImageRequestDto, fileList.size());


        return RegisterImageResponseDtoMapper.INSTANCE.mapList(
                imageService.registerImage(
                        RegisterImageParamDtoMapper.INSTANCE.map(
                                registerImageRequestDto
                        ),
                        fileList
                )
        );
    }

    @ResponseBody
    @GetMapping(value = "/{imageId}", produces = MediaType.IMAGE_JPEG_VALUE)
    public Resource downloadRepresentImage(@PathVariable UUID imageId) throws MalformedURLException {
        return new UrlResource("file:" + imageStore.getFullPath(imageId.toString()));
    }
}