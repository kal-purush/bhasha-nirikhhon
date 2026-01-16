package com.common.tools;

import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import com.sun.image.codec.jpeg.JPEGImageEncoder;
import com.sun.image.codec.jpeg.JPEGCodec;
import javax.imageio.ImageIO;

/**
 * 提供对原始图片等比例压缩的功能
 * <br/>Date: 2015-02-28
 * @author hyq
 */
public class ImageCompressor {
	private Image img;
	private int width,height;
	/** 
     * 构造函数 
     */  
    public ImageCompressor(String sourceFileName)  {  
        try {
			File file = new File(sourceFileName);// 读入文件  
			img = ImageIO.read(file);      // 构造Image对象  
			width = img.getWidth(null);    // 得到源图宽  
			height = img.getHeight(null);  // 得到源图长  
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }  
    /** 
     * 按照宽度还是高度进行压缩 
     * @param w int 最大宽度 
     * @param h int 最大高度 
     */  
    public void resizeFix(int w, int h,String targetFileName) throws IOException {  
        if (width / height > w / h) {  
            resizeByWidth(w,targetFileName);  
        } else {  
            resizeByHeight(h,targetFileName);  
        }  
    }  
    /** 
     * 以宽度为基准，等比例放缩图片 
     * @param w int 新宽度 
     */  
    private void resizeByWidth(int w,String targetFileName) throws IOException {  
        int h = (int) (height * w / width);  
        resize(w, h,targetFileName);  
    }  
    /** 
     * 以高度为基准，等比例缩放图片 
     * @param h int 新高度 
     */  
    private void resizeByHeight(int h,String targetFileName) throws IOException {  
        int w = (int) (width * h / height);  
        resize(w, h,targetFileName);  
    }  
    /** 
     * 强制压缩/放大图片到固定的大小 
     * @param w int 新宽度 
     * @param h int 新高度 
     */  
    private void resize(int w, int h,String targetFileName) throws IOException {  
        // SCALE_SMOOTH 的缩略算法 生成缩略图片的平滑度的 优先级比速度高 生成的图片质量比较好 但速度慢  
        BufferedImage image = new BufferedImage(w, h,BufferedImage.TYPE_INT_RGB );   
        image.getGraphics().drawImage(img, 0, 0, w, h, null); // 绘制缩小后的图   
        FileOutputStream out = new FileOutputStream(targetFileName); // 输出到文件流  
        // 可以正常实现bmp、png、gif转jpg  
        JPEGImageEncoder encoder = JPEGCodec.createJPEGEncoder(out);  
        encoder.encode(image); // JPEG编码  
        out.close();  
    }  

}