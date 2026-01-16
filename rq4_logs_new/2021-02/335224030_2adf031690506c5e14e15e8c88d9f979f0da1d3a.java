import java.awt.*;
import java.awt.image.*;
import javax.imageio.*;
import java.io.*;

public class Keskmine {
    public static double AritKesk(double a, double b, double c) {
        double summa = a + b + c;
        double keskmine = summa / 3;
        return keskmine;
    }
    public static double[] LibiKesk(double[] sisend) {
        double[] valjund = new double[sisend.length - 2];
        for(int i = 0; i < (sisend.length - 2); i++) {
            double summa = sisend[i] + sisend[i + 1] + sisend[i + 2];
            double keskmine = summa / 3;
            valjund[i] = keskmine;
        }
        return valjund;
    }
    public static void Joonista(double[] sisend, double[] vastus) throws Exception {
        int vahe = 5;
        int korgus = 20;
        int number = 5;
        BufferedImage pilt=new BufferedImage((sisend.length * 5) + sisend.length - 1, korgus, BufferedImage.TYPE_INT_RGB);
        Graphics g=pilt.createGraphics();
        g.setColor(Color.RED);
        for(int i = 0; i < sisend.length; i++) {
            double y = sisend[i];
            g.fillRect(vahe, 20 - (int)y, 1, 1);
            vahe += 5;
        }
        g.setFont(new Font("Courier New", 1, 7));
        for(int i = korgus; i > 0; i -= 5) {
            g.drawString(String.valueOf(number), 0, i);
            number += 5;
        }
        /*g.drawString("5", 0, 5);
        g.drawString("10", 0, 10);*/

        /*g.fillRect(5, 17, 1, 1);
        g.fillRect(10, 12, 1, 1);
        g.fillRect(15, 8, 1, 1);
        g.fillRect(20, 15, 1, 1);
        g.fillRect(25, 16, 1, 1);
        g.fillRect(30, 13, 1, 1);
        g.fillRect(35, 11, 1, 1);*/
        ImageIO.write(pilt, "png", new File("pilt1.png"));
    }
}