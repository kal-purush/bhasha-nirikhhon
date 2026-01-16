/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package multimedia;

import java.awt.AlphaComposite;
import java.awt.Dimension;
import java.awt.EventQueue;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.image.BufferedImage;
import java.io.IOException;
import javax.imageio.ImageIO;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.Timer;

/**
 *
 * @author Mostafa Aboelnasr
 */
public class Alpha {

    public static void main(String[] args) {
        new Alpha();
    }

    public Alpha() {
        EventQueue.invokeLater(new Runnable() {
            @Override
            public void run() {
                JFrame frame = new JFrame("Testing");
                frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
                frame.add(new TestPane());
                frame.pack();
                frame.setLocationRelativeTo(null);
                frame.setVisible(true);
            }
        });
    }

    public static class TestPane extends JPanel {

        public static final long RUNNING_TIME = 0000;

        private BufferedImage startImage;
        private BufferedImage secondImage;

        private float alpha = 0f;
        private long startTime = -1;

        public TestPane() {
            try {
                startImage = ImageIO.read(getClass().getResource("/multimedia/src/dema.jpg")).getSubimage(100, 100, 300, 300);
                secondImage = ImageIO.read(getClass().getResource("/multimedia/src/park.jpg")).getSubimage(100, 100, 300, 300);
            } catch (IOException exp) {
            }

            final Timer timer = new Timer(1, new ActionListener() {
                @Override
                public void actionPerformed(ActionEvent e) {
                    if (startTime < 0) {
                        startTime = System.currentTimeMillis();
                    } else {

                        long time = System.currentTimeMillis();
                        long duration = time - startTime;
                        if (duration >= RUNNING_TIME) {
                            startTime = -1;
                            ((Timer) e.getSource()).stop();
                            alpha = 0f;
                        } else {
//                            alpha = 1f - ((float) duration / (float) RUNNING_TIME);
                            alpha = 1f - .5f;
                        }
                        repaint();
                    }
                }
            });
            addMouseListener(new MouseAdapter() {

                @Override
                public void mouseClicked(MouseEvent e) {
                    alpha = 0f;
                    BufferedImage tmp = startImage;
                    startImage = secondImage;
                    secondImage = tmp;
                    timer.start();
                }

            });
        }

        @Override
        public Dimension getPreferredSize() {
            return new Dimension(
                    Math.max(startImage.getWidth(), secondImage.getWidth()),
                    Math.max(startImage.getHeight(), secondImage.getHeight()));
        }

        @Override
        protected void paintComponent(Graphics g) {
            super.paintComponent(g);
            Graphics2D g2d = (Graphics2D) g.create();
            g2d.setComposite(AlphaComposite.SrcOver.derive(.5f));
            int x = (getWidth() - startImage.getWidth());
            int y = (getHeight() - startImage.getHeight());
//            g2d.drawImage(startImage, x, y, this);

            g2d.setComposite(AlphaComposite.SrcOver.derive(1f - .5f));
            x = (getWidth() - secondImage.getWidth());
            y = (getHeight() - secondImage.getHeight());
            g2d.drawImage(secondImage, x, y, this);
            g2d.dispose();
        }

    }

}