/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Domain;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import javafx.scene.image.Image;

/**
 *
 * @author Steven
 */
public class FifthLane extends Character {
    
   

    public FifthLane(String name, int x, int y, int imgNum, int speed) throws FileNotFoundException {
      super(name, x, y, imgNum, speed);
        setSprite();
           }
    public void setSprite() throws FileNotFoundException {
        ArrayList<Image> sprite = super.getSprite();
        for (int i = 0; i < 6; i++) {
            if (super.getSpeed() == Utility.Variables.SlOW) {
                sprite.add(new Image(new FileInputStream("src/Assets/bill" + i + ".gif")));
            } else if (super.getSpeed() == Utility.Variables.QUICK) {
                sprite.add(new Image(new FileInputStream("src/Assets/Ryu" + i + ".gif")));
            } else if (super.getSpeed() == Utility.Variables.MEDIUM) {
                sprite.add(new Image(new FileInputStream("src/Assets/mega" + i + ".gif")));
            } else if (super.getSpeed() == Utility.Variables.RANDOM) {
                sprite.add(new Image(new FileInputStream("src/Assets/Running" + i + ".png")));
            }
        }
        super.setSprite(sprite);
    }

    @Override
    public void run() {
        ArrayList<Image> sprite = super.getSprite();
        int x = 0;
        int bandera = 1;
        while (true) {
            try {
                switch (bandera) {
                    case 1:
                        for (int i = 200; i < 520; i = i + 15) {
                            for (int j = 0; j < 3; j++) {

                                super.setImage(sprite.get(j));
                                Thread.sleep(this.getSpeed());
                                super.setY(i);
                            }
                        }
                        bandera = 2;
                        break;

                    case 2:
                        for (int i = 750; i > 20; i = i - 15) {
                            for (int j = 3; j < 6; j++) {
                                super.setImage(sprite.get(j));
                                Thread.sleep(100);
                                super.setX(i);
                                bandera = 3;

                            }
                        }
                        break;

                    case 3:
                        if (super.isWall()) {
                            suspend();
                        }
                        bandera = 4;

                        break;
                    case 4:
                        for (int i = 385; i >= 5; i = i - 15) {
                            for (int j = 0; j < 3; j++) {
                                super.setImage(sprite.get(j));
                                Thread.sleep(100);
                                super.setY(i);
                                bandera = 5;

                            }
                        }
                        break;

                    case 5:
                        for (int i = 140; i < 900; i = i + 15) {
                            for (int j = 0; j < 3; j++) {
                                super.setImage(sprite.get(j));
                                Thread.sleep(100);
                                super.setX(i);
                                bandera = 6;

                            }
                        }
                        break;
                    case 6:
                        for (int i = 5; i < 220; i = i + 15) {
                            for (int j = 3; j < 6; j++) {
                                super.setImage(sprite.get(j));
                                Thread.sleep(100);
                                super.setY(i);
                                bandera = -1;

                            }
                        }
                        break;

                    default:
                        break;
                }

            } catch (InterruptedException ex) {
            }
        }//while
    }//run
}