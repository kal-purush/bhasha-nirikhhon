package com.example.enigmator.entity;

import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.MultipartBuilder;
import com.squareup.okhttp.RequestBody;

import java.io.File;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class Media {
    private String file;
    private String mediaType;

    public static RequestBody buildRequestBody(Media media) {
        MultipartBuilder builder = new MultipartBuilder();
        builder.type(MultipartBuilder.FORM);
        builder.addFormDataPart("mediaType", media.getMediaType());
        builder.addFormDataPart("file", media.getFile(), RequestBody.create(mediaTypeFromMedia(media), new File(media.getFile())));

        return builder.build();
    }

    private static MediaType mediaTypeFromMedia(Media media) {
        String extension = media.getFile().substring(media.getFile().lastIndexOf(".") + 1);
        String type;
        switch (extension.toLowerCase()) {
            case "png":
                type = "image/png";
                break;
            case "jpeg":
                type = "image/jpeg";
                break;
            case "jpg":
                type = "image/jpeg";
                break;
            case "mp4":
                type = "video/mp4";
                break;
            default:
                throw new UnsupportedOperationException("File extension not supported: " + extension);
        }

        return MediaType.parse(type);
    }
}