package alien;

import java.awt.*;
import javax.swing.*;

import main.Bomb;
import main.GameObject;
import main.Item;
import player.player;

public class normal extends GameObject {
	public boolean bye = false;
	public int score;
	private int bombcd = 70;
	private int bombcount = 0;
	private int bring = 0;

	public normal(int x, int y, double dx, double dy, int w, int h, int hp, int atk, double ax, double ay, int score) {
		this.score = score;
		this.x = x;
		this.y = y;
		this.dx = dx;
		this.dy = dy;
		this.colx = 2;
		this.coly = 4;
		this.colwidth = w - 4;
		this.colheight = h - 8;
		this.hp = hp;
		this.width = w;
		this.height = h;
		this.atk = atk;
		this.ax = ax;
		this.ay = ay;
		int randbring = (int) (Math.random() * 1000);
		if (randbring < 52)
			bring = 1;
		else if (randbring < 67)
			bring = 2;
		else if (randbring < 72)
			bring = 3;
		else if (randbring < 82)
			bring = 4;
		else if (randbring < 97)
			bring = 5;
		else if (randbring < 100)
			bring = 6;
		maxhp = hp;
		img = new ImageIcon(getClass().getResource("/source/a/PNG/Enemies/enemyRed2.png"));
	}

	@Override
	public void move() {
		super.move();
		if (x <= -width || y <= -height || x >= 600 || hp <= 0)
			bye = true;
	}

	@Override
	public void drawObject(Graphics g) {
		super.drawObject(g);
		g.setColor(Color.RED);
		g.fillRect(x + 4, y - 3, width - 8, 6);
		g.setColor(Color.GREEN);
		g.fillRect(x + 4, y - 3, (int) ((float) (width - 8) * ((float) hp / maxhp)), 6);
	}

	public Bomb getBomb() {
		return new Bomb(x + width / 2 - 4, y, atk);
	}

	public boolean doShot() {
		if (bombcount % bombcd == 0) {
			if (Math.random() <= 0.001) {
				bombcount = 0;
				return true;
			} else {
				return false;
			}
		} else {
			bombcount++;
			return false;
		}
	}

	public void setImg(String source) {
		img = new ImageIcon(getClass().getResource("/source/" + source));
	}

	@Override
	public void explodeObject(Graphics g) {
		// TODO Auto-generated method stub

	}

	public Item drop() {
		if (bring != 0)
			return new Item(bring, x, y);
		else
			return null;
	}

	public boolean doCrashed(player p) {
		return (x > p.getX() && y > p.getY() && x < p.getX() + p.getWidth() && y < p.getY() + p.getHeight())
				|| (x + width > p.getX() && y > p.getY() && x + width < p.getX() + p.getWidth()
						&& y < p.getY() + p.getHeight())
				|| (x + width > p.getX() && y + height > p.getY() && x + width < p.getX() + p.getWidth()
						&& y + height < p.getY() + p.getHeight())
				|| (x > p.getX() && y + height > p.getY() && x < p.getX() + p.getWidth()
						&& y + height < p.getY() + p.getHeight());
	}

}