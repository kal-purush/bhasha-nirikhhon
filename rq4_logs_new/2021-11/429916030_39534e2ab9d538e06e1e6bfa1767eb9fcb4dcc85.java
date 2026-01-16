package io.eholland;

import io.eholland.math.PerlinNoise;
import io.eholland.shapes.Line;

public class AppPerlinLineLoop {
    public static void main(String[] args) {
        int height = 900;
        int width = 900;

        float x, y;
        float lastX = 0;
        float lastY = 0;
        int xStep = 4;

        float yNoise =
                (float) Math.floor(Math.random() * 10);

        DrawableArray shapeArray = new DrawableArray();

//        for (int i = 0; i < 99; i++) {
        for (x = 20; x <= 880; x += xStep) {
            y = (float) (10 + PerlinNoise.noise(x, yNoise, 1) * 80);
            System.out.println("X: " + x + " Y: " + y + " lastX: " + lastX +
                                       " lastY: " + lastY);
            if (lastX > 0) {
                Line line = new Line((int) x, (int) y, (int) lastX,
                                     (int) lastY);
                shapeArray.add(line);
            }
//            try {
//                Thread.sleep(100);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
            lastX = x;
            lastY = y;
            yNoise += 0.03;
        }
//        }


        Gui gui = new Gui(height, width);
        gui.add(shapeArray);

        System.out.println(shapeArray);

    }
}