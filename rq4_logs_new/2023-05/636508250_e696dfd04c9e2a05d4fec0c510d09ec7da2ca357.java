package main;

import java.awt.image.BufferedImage;
import java.io.*;

public class Config {
    GamePanel gp;
    public Config(GamePanel gp){
        this.gp = gp;
    }
    public void savaConfig(){
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter("config.txt"));
            //Full screen setting
            if(gp.fullScreenOn) bw.write("On");
            if(!gp.fullScreenOn) bw.write("Off");
            bw.newLine();

            //Music volume
            bw.write(String.valueOf(gp.music.volumeScale));
            bw.newLine();

            //SE volume
            bw.write(String.valueOf(gp.se.volumeScale));
            bw.newLine();


            bw.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public void loadConfig(){
        try {
            BufferedReader br = new BufferedReader(new FileReader("config.txt"));

            String s = br.readLine();

            if(s.equals("On")) gp.fullScreenOn = true;
            if(s.equals("Off")) gp.fullScreenOn = false;

            //restore music volume
            s = br.readLine();
            gp.music.volumeScale = Integer.parseInt(s);
            s = br.readLine();
            gp.se.volumeScale = Integer.parseInt(s);

            br.close();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}