package com.bodytok.healthdiary.util;

import lombok.NoArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import java.util.Objects;
import java.util.UUID;

@Component
@NoArgsConstructor
public class FileNameConverter {

    public String convertFileName(MultipartFile file){
        String originalFileName = StringUtils.cleanPath(Objects.requireNonNull(file.getOriginalFilename()));
        String fileNameWithoutExtension = StringUtils.stripFilenameExtension(originalFileName);
        String extension = StringUtils.getFilenameExtension(originalFileName);
        return fileNameWithoutExtension + "_" + UUID.randomUUID() + "." + extension;
    }
}