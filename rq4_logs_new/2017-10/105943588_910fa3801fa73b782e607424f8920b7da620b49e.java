package kruskal;

import common.*;
import common.Set;

import java.applet.*;
import java.awt.*;
import java.util.*;

public class MainSquare extends Applet {
	
	Graphics g;
	
	int w = 64, h = 36, scale = 25;
	int drawType = 1;
	
	double delay = 0;
	double delayInc = 0;
	
	Random rng = new Random();
	
	boolean showSetup = true;
	
	Square[][] grid = new Square[w][h];
	ArrayList<Set> sets = new ArrayList<Set>();
	
	private void createGrid() {
		
		int setCount = 0;
		
		for (int i=0; i<w; i++) {
			for (int j=0; j<h; j++) {
				this.grid[i][j] = new Square(i, j, this.scale, setCount);
				this.sets.add(new Set(this.grid[i][j], setCount));
				setCount++;
			}
		}
	}
	
	private void createMaze() {
		
		while (sets.size() > 1) {
			
			this.delay += this.delayInc;
			
			if (showSetup) {
				if (this.drawType == 0)
					this.drawMaze();
				else
					this.drawBySets();
				try {
					Thread.sleep((long) this.delay);
				} catch (Exception e) {
					e.toString();
				}
			}
			removeWall();
		}
		
	}
	
	private int findSetIndex(int id) {
		for (int i=0; i<this.sets.size(); i++) {
			if (this.sets.get(i).id == id) {
				return i;
			}
		} return -1;
	}
	
	private void removeWall() {
		
		int[] wall = findWall();
		int x1 = wall[0], y1 = wall[1], x2 = wall[3], y2 = wall[4];
		
		this.grid[x1][y1].changeWall(wall[2], 0);
		this.grid[x2][y2].changeWall(wall[5], 0);
		
		int setId1 = this.grid[x1][y1].setId;
		int setIndex1 = findSetIndex(setId1);
		
		int setId2 = this.grid[x2][y2].setId;
		int setIndex2 = findSetIndex(setId2);
		
		this.sets.get(setIndex1).merge(this.sets.get(setIndex2));
		this.sets.remove(setIndex2);
		
	}
	
	private int[] findWall() {
		
		int x1 = rng.nextInt(this.w);
		int y1 = rng.nextInt(this.h);
		int side1 = rng.nextInt(4);
		
		int x2 = 0, y2 = 0, side2 = (side1 + 2) % 4;
		
		if (x1 == 0 && side1 == 3)
			return findWall();
		if (y1 == this.h - 1 && side1 == 2)
			return findWall();
		if (x1 == this.w - 1 && side1 == 1)
			return findWall();
		if (y1 == 0 && side1 == 0)
			return findWall();
		
		Square s = this.grid[x1][y1];
		Square n = null;
		
		switch (side1) {
			case 0: n = this.grid[x1][y1 - 1];
					x2 = x1; y2 = y1 - 1;
				break;
			case 1: n = this.grid[x1 + 1][y1];
					x2 = x1 + 1; y2 = y1;
				break;
			case 2: n = this.grid[x1][y1 + 1];
					x2 = x1; y2 = y1 + 1;
				break;
			case 3: n = this.grid[x1 - 1][y1];
					x2 = x1 - 1; y2 = y1;
				break;
		}
		
		if (s.setId == n.setId)
			return findWall();
		
		int[] output = {x1, y1, side1, x2, y2, side2};
		
		return output;
		
	}
	
	private boolean hasBeenVisited(int x, int y) {
		if (this.grid[x][y].walls.equals("1111"))
			return false;
		return true;
	}
	
	public void init() {
		setSize(w * scale, h * scale);
		setBackground(Color.BLACK);
		createGrid();
	}

	public void paint(Graphics g) { this.g = g;
		this.createMaze();
		if (this.drawType == 0)
			this.drawMaze();
		else
			this.drawBySets();
	}
	
	private void drawMaze() {
		for (int i = 0; i < this.w; i++) {
			for (int j = 0; j < this.h; j++) {
				if (hasBeenVisited(i, j))
					this.grid[i][j].draw(g, Color.BLACK);
				else
					this.grid[i][j].draw(g, Color.GRAY);
			}
		}
	}
	
	private void drawBySets() {
		for (int i=0; i<this.sets.size(); i++) {
			this.sets.get(i).drawSet(g);
		}
	}

}