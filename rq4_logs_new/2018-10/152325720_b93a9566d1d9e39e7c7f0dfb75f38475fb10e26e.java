package ktu.tanks.entities;

import ktu.tanks.Direction;
import ktu.tanks.decorators.Renderable;
import ktu.tanks.entities.base.PlayerEntity;
import ktu.tanks.util.ImageLoader;

import java.awt.*;

public class Tank extends PlayerEntity implements Renderable {

    public Tank(int x, int y, Direction direction, int movementSpeed, String playerName, int health) {
        super(playerName);

        this.x = x;
        this.y = y;
        this.direction = direction;
        this.movementSpeed = movementSpeed;

        this.playerName = playerName;
        this.health = health;
    }

    @Override
    protected void loadImages() {
        this.imgUp = ImageLoader.loadImage("tank/t1n.png");
        this.imgDown = ImageLoader.loadImage("tank/t1s.png");
        this.imgLeft = ImageLoader.loadImage("tank/t1w.png");
        this.imgRight = ImageLoader.loadImage("tank/t1e.png");
    }

    public void render(Graphics g) {
        System.out.println("Rendering " + playerName);
        g.drawImage(getImage(), x, y, null);
    }

}