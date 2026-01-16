package controllers;

import play.Play;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

/**
 * @author fblarel
 *         Date: 07/07/15
 */
public class MockPictureSaver implements PictureSaver {

    public static final String TEMPFILE = "/tmp/tempfile.png";
    public static final String TEMPFILE2 = "/public/images/";
    public static final String TEMP_PICTURE_PNG = "tempPicture.png";

    @Override
    public void takePicture() throws IOException {
        try {
            // determine le taille courante du screen
            Toolkit toolkit = Toolkit.getDefaultToolkit();
            Dimension screenSize = toolkit.getScreenSize();
            Rectangle screenRect = new Rectangle(screenSize);
            //creer le screenshot
            Robot robot = new Robot();
            BufferedImage image = robot.createScreenCapture(screenRect);
            // sauvegarde de l'image vers un fichier "png"
            File publicDir = Play.application().getFile(TEMPFILE2);
            File file = new File(publicDir.getAbsolutePath() + "/" + TEMP_PICTURE_PNG);
            ImageIO.write(image, "png", file);
            System.out.println("Capture screen saved in " + file.getAbsolutePath());
        }catch (AWTException awt){
            throw new IOException(awt);
        }
    }
}