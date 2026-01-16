package kcl.paramount.group.business;

import kcl.paramount.group.util.JSONUtils;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;

public class FileBusiness {

    private static final String ROOT_PATH = "/home/upload/";

    /*
        check target directory existed or not
        if not => create it
        upload file
     */
    public String upload(String username, MultipartFile file, String url) {
        String result = null;
        if(file.isEmpty()) {
            result = JSONUtils.getJSONString("fail", "file is empty");
        }
        else {
            String filePath;
            if (url.startsWith("/")) {
                filePath = ROOT_PATH + username + url;
            }
            else {
                filePath = ROOT_PATH + username + "/" + url;
            }
            if (filePath.endsWith("/") == false) {
                filePath = filePath + "/";
            }
            if (judegeDirExisted(filePath) == false) {
                createDirectory(filePath);
            }

            String fileName = file.getOriginalFilename();
            File dest = new File(filePath + fileName);
            try {
                file.transferTo(dest);
                result = JSONUtils.getJSONString("success", "");
            } catch (IOException e) {
                result = JSONUtils.getJSONString("fail", "unknown reasons");
            }
        }
        return result;
    }

    //judge the target directory existed or not
    private Boolean judegeDirExisted(String url) {
        Boolean result = false;
        File file = new File(url);
        if (file.exists()) {
            if (file.isDirectory()) {
                result = true;
            }
        }
        return result;
    }

    //create target directories
    private void createDirectory(String url) {
        File file = new File(url);
        file.mkdirs();
    }

}