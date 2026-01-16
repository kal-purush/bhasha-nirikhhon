package game_or_fun_new;

import javax.swing.*;
import java.awt.*;

public class BearCarImage extends JPanel {
    public void paintComponent(Graphics g) {
        super.paintComponent(g);

        // 绘制小熊汽车的车身
        g.setColor(Color.ORANGE);
        g.fillRect(100, 50, 200, 100);

        // 绘制小熊汽车的轮子
        g.setColor(Color.BLACK);
        g.fillOval(120, 150, 40, 40);
        g.fillOval(240, 150, 40, 40);

        // 绘制小熊汽车的眼睛和嘴巴
        g.setColor(Color.WHITE);
        g.fillOval(130, 70, 50, 50);
        g.fillOval(220, 70, 50, 50);
        g.setColor(Color.BLACK);
        g.fillOval(145, 85, 20, 20);
        g.fillOval(235, 85, 20, 20);
        g.fillOval(185, 110, 30, 20);

        // 绘制小熊汽车的耳朵和手
        g.setColor(Color.ORANGE);
        g.fillOval(80, 60, 50, 70);
        g.fillOval(270, 60, 50, 70);
        g.fillOval(50, 90, 50, 50);
        g.fillOval(310, 90, 50, 50);
    }

    public static void main(String[] args) {
        JFrame frame = new JFrame("Bear Car Image");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.add(new BearCarImage());
        frame.setSize(400, 250);
        frame.setVisible(true);
    }
}