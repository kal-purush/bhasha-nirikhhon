package org.example.common.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;
import java.util.Random;

import org.springframework.util.StringUtils;

/**
 * @author walle
 * @version V1.0
 * @since 2020-12-07 11:15
 */
public class FileUtil {

    private final static Random RANDOM = new Random();

    /**
     * 特殊文件类型
     */
    public static final String[] COMPLEX_FILE_SUFFIX_NAME_ARR = { "tar.gz" };


    /**
     * 创建唯一文件名称(fake news)
     *
     * @param originalFileName 文件名称
     * @return String
     */
    public static String buildUniqueFileName(String originalFileName) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        String suffix = FileUtil.getSuffix(originalFileName);

        StringBuilder nameBuilder = new StringBuilder();
        nameBuilder.append(sdf.format(new Date()));
        nameBuilder.append(String.format("%04d", RANDOM.nextInt(1000)));
        nameBuilder.append(".");
        nameBuilder.append(suffix);

        return nameBuilder.toString();
    }


    /**
     * 获取文件后缀名称
     *
     * @param fileName 文件名称
     * @return String
     */
    public static String getSuffix(String fileName) {
        if (StringUtils.isEmpty(fileName)) {
            return null;
        }

        String suffix = null;
        for (String sub : COMPLEX_FILE_SUFFIX_NAME_ARR) {
            if (fileName.endsWith(sub)) {
                suffix = sub;
                break;
            }
        }
        if (Objects.isNull(suffix)) {
            int index = fileName.lastIndexOf(".");
            suffix = fileName.substring(index + 1);
        }

        return suffix;
    }
}