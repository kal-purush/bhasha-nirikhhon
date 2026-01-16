package demo.simulation;

import java.awt.Point;

public class Battleground {
	int width;
	int height;
	
	InnocentPrey[][] field;
	
	Killer wolfInWolvesClothing;
	
	public Battleground(int w_, int h_) {
		width = w_;
		height = h_;
		
		field = new InnocentPrey[width][height];
		field = generateMap(width, height);
		
		wolfInWolvesClothing = new Killer(width / 2, height / 2);
	}
	
	public InnocentPrey[][] generateMap(int w, int h) {
		InnocentPrey[][] map = new InnocentPrey[w][h];
		
		for(int x = 0; x < w; x++) {
			for(int y = 0; y < h; y++) {
				if(Math.random() < 0.1)
					map[x][y] = new InnocentPrey();
				else
					map[x][y] = null;
			}
		}
		
		return map;
	}
	
	public InnocentPrey preyAtPos(Point pos) { return field[pos.x][pos.y]; }
	
	public boolean checkDone() {
		for(int x = 0; x < width; x++) {
			for(int y = 0; y < height; y++) {
				if(field[x][y] != null)
					return field[x][y].alive;
			}
		}
		
		return true;
	}
	
	public boolean iterate() {
		wolfInWolvesClothing.move(width, height);
		
		InnocentPrey prey = preyAtPos(wolfInWolvesClothing.pos);
		
		if(prey != null)
			wolfInWolvesClothing.kill(prey);
		
		return checkDone();
	}
}