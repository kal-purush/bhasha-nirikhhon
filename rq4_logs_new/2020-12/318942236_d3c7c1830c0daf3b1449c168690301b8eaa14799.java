package com.wyz.pms.common.util;

import com.wyz.pms.common.constant.PMSConstant;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

public class FileUtil {


    /**
     * @Title: upload
     * @Description: 单文件上传
     * @param file 文件信息
     * @return
     */
    public static String upload(MultipartFile file) {
        try {
            if (file==null || file.isEmpty()) {
                //throw new ParameterException("文件为空");
                return null;
            }
            // 获取文件名
            String fileName = file.getOriginalFilename();
            // 获取文件的后缀名
            String suffixName = fileName.substring(fileName.lastIndexOf("."));
            // 设置文件存储路径
            String fName = UUID.randomUUID().toString().replace("-", "");
            //拼接路径
            String path = PMSConstant.IMG_URL + fName + suffixName;
            File dest = new File(path);
            if (!dest.getParentFile().exists()) { // 检测是否存在目录
                dest.getParentFile().mkdirs();// 新建文件夹
            }
            file.transferTo(dest);// 文件写入
            String imgName = fName + suffixName;
            System.out.println("图片名称："+imgName);
            return imgName;//图片名称
        } catch (IllegalStateException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        //throw new ParameterException("上传失败");
        return null;
    }
}