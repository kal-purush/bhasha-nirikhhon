package com.fisco.app.utils;

import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * 文件上传工具集
 */
public class FileUploadUtils {

    private FileUploadUtils() {

    }

    /**
     * 上传文件到指定路径
     * @param file 上传的文件
     * @param dir 上传的文件夹路径
     * @return 是否上传成功
     */
    public static boolean saveFile(MultipartFile file, String dir) {
        String fileName = StringUtils.cleanPath(file.getOriginalFilename());
        try {
            Path path = Paths.get(dir + fileName);
            Files.copy(file.getInputStream(), path, StandardCopyOption.REPLACE_EXISTING);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static String getFilePath(MultipartFile file, String dir) {
        String fileName = StringUtils.cleanPath(file.getOriginalFilename());
        Path path = Paths.get(dir + fileName);
        return path.toString();
    }
}