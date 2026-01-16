import javax.swing.*;
import java.awt.*;
import java.io.File;
import java.io.IOException;
import javax.imageio.ImageIO;
import java.util.Random;

// Abstract parent class: HouseBase
abstract class HouseBase extends JPanel {
    protected BufferedImage image;

    public HouseBase() {
        loadImage();
    }

    protected abstract void loadImage();

    @Override
    protected void paintComponent(Graphics g) {
        super.paintComponent(g);
        if (image != null) {
            g.drawImage(image, 0, 0, this);
        }
    }
}

// Child class: Adds walls
class HouseWithWalls extends HouseBase {
    @Override
    protected void loadImage() {
        try {
            image = ImageIO.read(new File("walls.png"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

// Grandchild class: Adds windows
class HouseWithWindows extends HouseWithWalls {
    @Override
    protected void loadImage() {
        super.loadImage();
        try {
            BufferedImage windowsImage = ImageIO.read(new File("windows.png"));
            Graphics g = image.getGraphics();
            g.drawImage(windowsImage, 0, 0, null);
            g.dispose();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

// Great-grandchild class: Adds door
class HouseWithDoor extends HouseWithWindows {
    @Override
    protected void loadImage() {
        super.loadImage();
        try {
            BufferedImage doorImage = ImageIO.read(new File("door.png"));
            Graphics g = image.getGraphics();
            g.drawImage(doorImage, 0, 0, null);
            g.dispose();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

// Main class to display GUI
public class HouseGUI {
    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            JFrame frame = new JFrame("House GUI");
            frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
            frame.setSize(500, 500);

            HouseBase house = new HouseWithDoor(); // Change to any other class to see different stages
            frame.add(house);
            frame.setVisible(true);
        });
    }
}