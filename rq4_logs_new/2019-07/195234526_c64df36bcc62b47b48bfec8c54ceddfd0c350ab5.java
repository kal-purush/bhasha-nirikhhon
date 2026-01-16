package org.java.web;

import org.apache.commons.fileupload.disk.DiskFileItem;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.commons.CommonsMultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

@Controller
public class FixedLosss {

    @RequestMapping("Fixed")
    public String fixed(){
        return "FixedLoss/Insurance";
    }

    @RequestMapping("upload")
    @ResponseBody
    public Map upload(@RequestParam(value="file",required=false) MultipartFile file, HttpServletRequest request, HttpServletResponse response) throws Exception {

        String prefix="";
        String dateStr="";
        //保存上传
        OutputStream out = null;
        InputStream fileInput=null;
        String originalName="";
        try{
            if(file!=null){
                CommonsMultipartFile cfile= (CommonsMultipartFile) file;
                DiskFileItem dfile= (DiskFileItem) cfile.getFileItem();
                fileInput=dfile.getInputStream();
                //获得文件名
                originalName = file.getOriginalFilename();
                //获得文件后缀
                prefix=originalName.substring(originalName.lastIndexOf(".")+1);



            }
        }catch (Exception e){
        }finally{
            try {
                if(out!=null){
                    out.close();
                }
                if(fileInput!=null){
                    fileInput.close();
                }
            } catch (IOException e) {
            }
        }
        Map<String,Object> map2=new HashMap<>();
        Map<String,Object> map=new HashMap<>();
        map.put("code",0);
        map.put("msg","");
        map.put("data",map2);
        map2.put("src","../../../static"+originalName + dateStr + "." + prefix);
        return map;
    }
}