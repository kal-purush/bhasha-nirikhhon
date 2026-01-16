package com.metamong.mt.global.file;

import java.util.UUID;

import jakarta.validation.constraints.NotNull;

public abstract class AbstractFilenameResolver implements FilenameResolver {

    @Override
    public String generateUuidFilename(@NotNull String originalFilename) {
        if (originalFilename == null) {
            throw new NullPointerException("originalFilename이 null입니다.");
        }
        UUID uuid = UUID.randomUUID();
        return uuid + extractFileExtension(originalFilename);
    }
    
    private String extractFileExtension(String filename) {
        int indexOfExtensionSeparator = filename.lastIndexOf(".");
        if (indexOfExtensionSeparator == -1) {
            return "";
        }
        return filename.substring(indexOfExtensionSeparator);
    }

    @Override
    public String generateUuidFilename(@NotNull FileType fileType) {
        if (fileType == null) {
            throw new NullPointerException("fileType이 null입니다.");
        }
        return UUID.randomUUID() + fileType.extension();
    }
}