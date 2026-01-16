package com.github.classpick.file.upload.util;

import java.util.UUID;

public class S3KeyFactory {
    public static String reservatioOcrKey(Long reservationId){
        return "reservation/%d/ocr/%s".formatted(
                reservationId,
                UUID.randomUUID()
        );
    }

    public static String reservatioCleanUpKey(Long reservationId){
        return "reservation/%d/clean-up/%s".formatted(
                reservationId,
                UUID.randomUUID()
        );
    }
}