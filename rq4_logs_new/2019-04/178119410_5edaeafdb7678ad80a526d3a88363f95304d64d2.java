package Viewer;

import Model.*;
import Controller.*;


import javax.swing.*;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.lang.Math;

import javax.imageio.*;
import java.io.*;

class LawnPanel extends JPanel
{
    private int panelW = 600;
    private int panelH = 600;
    private int nRow = 8;
    private int nCol = 12;
    private int margin = 20;

    private int borderThickness = 2;


    //private String[][] lawn;
    private RealLawn lawn;
    private int squareSize = 1;

    private Image puppyImage;
    private Image craterImage;
    private Image mowerImage;
    private Image puppymowerImage;

    private int fontSize = 12;

    /*arguments: panel width, panel height, lawn width, lawn height, lawnMap[][], borderThickness, panel margin, fontsize*/
    public LawnPanel(int width, int height, int ncol, int nrow, RealLawn newLawn, int t, int m, int f)
    {
        panelW = width;
        panelH = height;

        setBorder(BorderFactory.createLineBorder(Color.black));

        nCol = ncol;
        nRow = nrow;
        borderThickness = t;
        margin = m;
        fontSize = f;

        LoadImages();

        lawn = new RealLawn(ncol, nrow);
        for (int i=0;i<nCol;i++)
        {
            for(int j=0;j<nRow;j++)
            {
                Location location = new Location(i, j);
                lawn.setSquare(location, newLawn.getSquareState(location));
            }
        }
    }

    //load images and resize to square size
    private void LoadImages()
    {
        GetSquareSize();
        try {
            //ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            //InputStream input = classLoader.getResourceAsStream(
            //        "resources/puppy.png");
            //if (input != null) {
            BufferedImage img = ImageIO.read(new File("resources/puppy.png"));
            puppyImage = img.getScaledInstance(squareSize, squareSize, Image.SCALE_DEFAULT);
            img = ImageIO.read(new File("resources/mower.png"));
            mowerImage = img.getScaledInstance(squareSize, squareSize, Image.SCALE_DEFAULT);
            img = ImageIO.read(new File("resources/crater.png"));
            craterImage = img.getScaledInstance(squareSize, squareSize, Image.SCALE_DEFAULT);
            img = ImageIO.read(new File("resources/mower_puppy.png"));
            puppymowerImage = img.getScaledInstance(squareSize, squareSize, Image.SCALE_DEFAULT);
            //}
            //else
            //{
            //    System.out.println("image not found");
            //}
        } catch (Exception e) {
        }
    }


    //update lawn map, size can change
    public void update(int lawnCol, int lawnRow, RealLawn newLawn)
    {
        //repaint();
        boolean sizeChanged = (lawnCol != nCol || lawnRow != nRow);

        nCol=lawnCol;
        nRow=lawnRow;

        if (sizeChanged)
        {
            GetSquareSize();
            LoadImages();
        }

        lawn = new RealLawn(nCol, nRow);
        //copy to lawn
        for (int i=0;i<nCol;i++)
        {
            for(int j=0;j<nRow;j++)
            {
                Location location = new Location(i, j);
                lawn.setSquare(location, newLawn.getSquareState(location));
            }
        }

        GetSquareSize();
        repaint();
    }

    public Dimension getPreferredSize() {
        return new Dimension(panelW,panelH);
    }

    //calculate size of a square based on canvas size, margin, border thickness and grid size
    private void GetSquareSize()
    {
        int width = (panelW - margin * 2 - borderThickness)/nCol -borderThickness;
        int height = (panelH - margin * 2 - borderThickness)/nRow -borderThickness;
        squareSize = Math.min(width, height);
    }

    //draw a string in the center of a rectangular space
    private void drawCenteredString(Graphics g, String text, Rectangle rect, Font font) {
        FontMetrics metrics = g.getFontMetrics(font);
        int x = rect.x + (rect.width - metrics.stringWidth(text)) / 2;
        int y = rect.y + ((rect.height - metrics.getHeight()) / 2) + metrics.getAscent();
        g.setFont(font);
        g.drawString(text, x, y);
    }

    //draw the content of a square
    private void drawSquare(Graphics g, int i, int j, Color bg, Image image){
        g.setColor(bg);
        g.fillRect((squareSize + borderThickness) * i + borderThickness + margin + 1,  (squareSize + borderThickness) * (nRow - j - 1) + borderThickness + margin + 1, squareSize, squareSize);
        if (image != null)
            g.drawImage(image, (squareSize + borderThickness) * i + borderThickness + margin + 1, (squareSize + borderThickness) * (nRow - j - 1) + borderThickness + margin + 1, null);
    }

    protected void paintComponent(Graphics g) {
        super.paintComponent(g);
        //g.drawString("This is my custom Panel!",10,20);
        GetSquareSize();

        //draw grid and indices
        g.setColor(Color.black);
        for (int j = 0; j < nCol; j++){
            drawCenteredString(g, String.format("%d", j), new Rectangle((squareSize + borderThickness) * j + borderThickness + margin, (squareSize + borderThickness) * nRow + borderThickness + margin + 1, squareSize, margin), new Font("TimesRoman", Font.BOLD, fontSize));
            g.fillRect((squareSize + borderThickness) * j + margin + 1, margin + 1, borderThickness, (squareSize + borderThickness) * nRow + borderThickness);
        }
        g.fillRect((squareSize + borderThickness) * nCol + margin + 1, margin + 1, borderThickness, (squareSize + borderThickness) * nRow + borderThickness);

        for (int i = 0; i < nRow; i++){
            drawCenteredString(g, String.format("%d", i), new Rectangle(0, (squareSize + borderThickness) * (nRow - i - 1) + borderThickness + margin, margin, squareSize), new Font("TimesRoman", Font.BOLD, fontSize));
            g.fillRect(margin + 1,(squareSize + borderThickness) * i + margin + 1, (squareSize + borderThickness) * nCol + borderThickness, borderThickness);
        }
        g.fillRect(margin + 1,(squareSize + borderThickness) * nRow + margin + 1, (squareSize + borderThickness) * nCol + borderThickness, borderThickness);

        //draw lawn squares
        for (int i = 0; i < nCol; i++) {
            for (int j = 0; j < nRow; j++) {
                SquareState currentSquareState = lawn.getSquareState(new Location(i, j));
                switch (currentSquareState)
                {
                    case grass:
                        drawSquare(g, i, j, new Color(0,153,0), null);
                        break;
                    case empty:
                        drawSquare(g, i, j, Color.white, null);
                        break;
                    case puppy_grass:
                        drawSquare(g, i, j, new Color(0,153,0), puppyImage);
                        break;
                    case puppy_empty:
                        drawSquare(g, i, j, Color.white, puppyImage);
                        break;
                    case mower:
                        drawSquare(g, i, j, Color.white, mowerImage);
                        break;
                    case puppy_mower:
                        drawSquare(g, i, j, Color.white, puppymowerImage);
                        break;
                    case crater:
                        drawSquare(g, i, j, Color.white, craterImage);
                        break;

                }


            }
        }

    }
}