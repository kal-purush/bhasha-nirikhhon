/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bitzgame.bitz;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.newdawn.slick.SlickException;

/**
 *
 * @author Timo
 */
public class Walls {

    GameObject[][] wallArray;
    int width;
    int height;

    public Walls() {
        width = 10;
        height = 10;
        wallArray = new GameObject[height][width];
        try {
            setupArray();
        } catch (SlickException ex) {
            Logger.getLogger(Walls.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("wall arrayn setuppaus meni pieleen");
        }
    }

    public GameObject[][] getWallArray() {
        return this.wallArray;
    }

    public void setupArray() throws SlickException {
        int[][] wallsMap = new int[height][width];
        Scanner scanner = null;
        try {
            scanner = new Scanner(new BufferedReader(new FileReader("src/assets/wallsMap.txt")));
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Walls.class.getName()).log(Level.SEVERE, null, ex);
        }
        String line;
        String[] numbers;
        int k = 0;
        for (int i = 0; i < height; i++) {
            line = scanner.nextLine();
            numbers = line.split(" ");
            for (int j = 0; j < width; j++) {
                k = Integer.parseInt(numbers[j]);
                wallsMap[i][j] = k;
                if (k == 1) {
                    wallArray[i][j] = new GameObject(i * 50, j * 50, "src/assets/spr_wall1.png");
                }
            }
        }
    }

}