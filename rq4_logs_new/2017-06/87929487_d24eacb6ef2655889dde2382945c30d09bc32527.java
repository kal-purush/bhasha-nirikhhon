package com.java.xdd.pdf;

import com.itextpdf.text.Image;
import com.itextpdf.text.Rectangle;
import com.itextpdf.text.pdf.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.List;

public class PdfTest {

    public static void main(String[] args) throws Exception {
        // 模板文件路径
        String templatePath = "G:\\temp.pdf";
        // 生成的文件路径
        String targetPath = "G:\\target.pdf";
        // 书签名
        String fieldName = "field";
        // 图片路径
        String imagePath = "G:\\image.jpg";

        // 读取模板文件
        InputStream input = new FileInputStream(new File(templatePath));
        PdfReader reader = new PdfReader(input);
        PdfStamper stamper = new PdfStamper(reader, new FileOutputStream(targetPath));
        // 提取pdf中的表单
        AcroFields form = stamper.getAcroFields();
        form.addSubstitutionFont(BaseFont.createFont("STSong-Light", "UniGB-UCS2-H", BaseFont.NOT_EMBEDDED));



        form.setField("test", "你好");


        /*// 通过域名获取所在页和坐标，左下角为起点
        List<AcroFields.FieldPosition> fieldPositions = form.getFieldPositions(fieldName);
        int pageNo = form.getFieldPositions(fieldName).get(0).page;
        Rectangle signRect = form.getFieldPositions(fieldName).get(0).position;
        float x = signRect.getLeft();
        float y = signRect.getBottom();
        // 读图片
        Image image = Image.getInstance(imagePath);
        // 获取操作的页面
        PdfContentByte under = stamper.getOverContent(pageNo);
        // 根据域的大小缩放图片
        image.scaleToFit(signRect.getWidth(), signRect.getHeight());
        // 添加图片
        image.setAbsolutePosition(x, y);
        under.addImage(image);*/

        stamper.close();
        reader.close();
    }
}