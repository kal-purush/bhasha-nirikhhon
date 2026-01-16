package com.company;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;

public class Main {

    public static void noiz (int porog)throws IOException {

        BufferedImage im = ImageIO.read(new File("C:\\Users\\5elementy\\IdeaProjects\\labgims\\file.jpg"));
        int rx=0;
        int ry=0;
        int newR =0;
        int newG = 0;
        int newB= 0;
        for (int i = 0; i < im.getHeight()*im.getWidth()*porog/100; i++) {
            rx = 0 + (int)(Math.random()*im.getHeight());
            ry = 0 + (int)(Math.random()*im.getWidth());
            newR = 1 + (int)(Math.random()*255);
            newG = 1 + (int)(Math.random()*255);
            newB = 1 + (int)(Math.random()*255);
            Color newCol = new Color(newR, newG, newB);

            im.setRGB(ry,rx,newCol.getRGB());

        }

        ImageIO.write(im,"jpg",new File("C:\\Users\\5elementy\\IdeaProjects\\labgims\\file1.jpg"));


    }


    public static void filtr ()throws IOException{
        int [] masR = new int[5];
        int [] masG = new int[5];
        int [] masB = new int[5];
        BufferedImage im = ImageIO.read(new File("C:\\Users\\5elementy\\IdeaProjects\\labgims\\file1.jpg"));


        for (int i = 1; i <im.getHeight() -1; i++) {
            for (int j = 1; j <im.getWidth()-1; j++) {

                    Color col = new Color(im.getRGB(j,i ));
                    Color col1 = new Color(im.getRGB(j,i + 1));
                    Color col2 = new Color(im.getRGB(j + 1,i ));
                    Color col3 = new Color(im.getRGB( j,i - 1));
                    Color col4 = new Color(im.getRGB( j - 1,i));
                    masR[0] = col.getRed();
                    masR[1] = col1.getRed();
                    masR[2] = col2.getRed();
                    masR[3] = col3.getRed();
                    masR[4] = col4.getRed();

                    masG[0] = col.getGreen();
                    masG[1] = col1.getGreen();
                    masG[2] = col2.getGreen();
                    masG[3] = col3.getGreen();
                    masG[4] = col4.getGreen();


                    masB[0] = col.getBlue();
                    masB[1] = col1.getBlue();
                    masB[2] = col2.getBlue();
                    masB[3] = col3.getBlue();
                    masB[4] = col4.getBlue();
                    Arrays.sort(masR);
                    Arrays.sort(masG);
                    Arrays.sort(masB);
                    Color newCOl = new Color(masR[2], masG[2], masB[2]);
                    im.setRGB(j,i, newCOl.getRGB());
            }

        }
        ImageIO.write(im,"jpg",new File("C:\\Users\\5elementy\\IdeaProjects\\labgims\\fileFilt.jpg"));

    }

    public static void main(String[] args)throws IOException  {
	noiz(15);
	filtr();
    }
}