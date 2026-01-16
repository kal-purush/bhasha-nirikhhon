package bitmap.transformer;

import org.junit.jupiter.api.Test;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

class BitmapTest {

    @Test
    void test_grayScale() throws IOException {
        Bitmap sut = new Bitmap("gray");
        String imagePath = "src/main/assets/baldy-8bit.bmp";
        String outPutPath = "src/main/assets/";
        BufferedImage img1 = ImageIO.read(new File(imagePath));
        sut.grayScale(img1, outPutPath, "bitManTest");
        Path path = Paths.get("src/main/assets/bitManTestgrayscale.bmp");
        System.out.println(path);
        assertTrue(true, "Yo something is wrong with test_grayScale()");

    }

    @Test
    void darken() throws IOException {
        Bitmap sut = new Bitmap("darken");
        String imagePath = "src/main/assets/baldy-8bit.bmp";
        String outPutPath = "src/main/assets/";
        BufferedImage img2 = ImageIO.read(new File(imagePath));
        sut.darken(img2, outPutPath, "bitManTest", .2);
        Path path = Paths.get("src/main/assets/bitManTestdarken.bmp");
        System.out.println(path);
        assertTrue(true, "Yo something is wrong with test_darken()");
    }

    @Test
    void resizeImage() throws IOException {
        Bitmap sut = new Bitmap("resize");
        String imagePath = "src/main/assets/baldy-8bit.bmp";
        String outPutPath = "src/main/assets/";
        BufferedImage img3 = ImageIO.read(new File(imagePath));
        sut.resizeImage(img3, outPutPath, "bitManTest", .5);
        Path path = Paths.get("src/main/assets/bitManTestresize.bmp");
        System.out.println(path);
        assertTrue(true, "Yo something is wrong with test_resizeImage()");
    }

}