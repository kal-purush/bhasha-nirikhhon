package io.github.ketao1989.image;

import org.im4java.core.CompositeCmd;
import org.im4java.core.IMOperation;
import org.im4java.core.ImageCommand;

import java.awt.image.BufferedImage;
import java.io.File;

import javax.imageio.ImageIO;

/**
 * @author tao.ke Date: 2017/9/14 Time: 下午10:22
 */
public class ImJavaImgTest {

  public static void main(String[] args) throws Exception {

    long start = System.currentTimeMillis();
    ImageCommand cmd = new CompositeCmd();

    // 原始图片信息
    BufferedImage buffimg = ImageIO.read(new File("/Users/ketao/demo/certificate.png"));
    int w = buffimg.getWidth();
    int h = buffimg.getHeight();

    IMOperation op = new IMOperation();
    BufferedImage watermarkImage = ImageIO.read(new File("/Users/ketao/demo/logo.png"));
    int ww = watermarkImage.getWidth();
    int wh = watermarkImage.getHeight();

    // 水印图片位置
    op.geometry(ww, wh, w - ww - 10, h - wh - 10);
    op.geometry(ww, wh, w - ww - 30, h - wh - 20);
    // 水印透明度
    op.dissolve(10);
    // 水印
    op.addImage("/Users/ketao/demo/logo.png");
    // 原图
    op.addImage("/Users/ketao/demo/certificate.png");
    // 目标
    op.addImage("test.png");

    cmd.run(op);
    System.out.println(System.currentTimeMillis() - start);
  }

}