package Util;

import sun.misc.BASE64Decoder;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileOutputStream;

public class Image {

  public int download(String url) {
    String path = "C:\\Users\\联想\\test.png";
    String result = "C:\\Users\\联想\\Image\\Sample\\result.jpg";
    try {
      BASE64Decoder decoder=new BASE64Decoder();
      FileOutputStream write = new FileOutputStream(new File(path));
      byte[] decoderBytes = decoder.decodeBuffer(url.split(",")[1]);
      write.write(decoderBytes);
      write.close();
      try{
        BufferedImage bufferedImage = ImageIO.read(new File(path));
        BufferedImage newBufferedImage = new BufferedImage(bufferedImage.getWidth(),
          bufferedImage.getHeight(),BufferedImage.TYPE_INT_RGB);
        newBufferedImage.createGraphics().drawImage(bufferedImage,0,0, Color.WHITE,null);
        ImageIO.write(newBufferedImage,"jpg",new File(result));
        System.out.println("Done");
      }catch (Exception e){
        e.printStackTrace();
      }
      return 1;
    } catch (Exception e) {
      e.printStackTrace();
      return 0;
    }
  }
}
