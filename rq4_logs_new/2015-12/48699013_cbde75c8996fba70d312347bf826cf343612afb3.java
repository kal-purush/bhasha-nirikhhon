package xsked.level;

import time.api.debug.Debug;
import time.api.entity.Entity;
import time.api.gfx.Mesh;
import time.api.gfx.Renderer;
import time.api.gfx.VertexTex;
import time.api.gfx.texture.Texture;

public class Chunk extends Entity {
	public static final int TILES = 5;
	public static final float SIZE = TILES * Tile.SIZE;
	
	private Tile[][] tiles;
	
	private Level level;
	
	private int[] indices;
	private VertexTex[] vertices;
	
	
	public Chunk(int x, int y, Level level) {
		this.level = level;
		
		indices = new int[TILES * TILES * 6];
		vertices = new VertexTex[TILES * TILES * 4];
		
		tiles = new Tile[TILES][TILES];
		for(int j = 0; j < TILES; j++) {
			for(int i = 0; i < TILES; i++) {
				Tile t = new Tile(level, i, j, 0, false, true);
				tiles[j][i] = t;
				VertexTex[] v = t.getVertices();
				
				indices[(i + TILES * j) * 6 + 0] = (i + TILES * j) * 4 + 0;
				indices[(i + TILES * j) * 6 + 1] = (i + TILES * j) * 4 + 1;
				indices[(i + TILES * j) * 6 + 2] = (i + TILES * j) * 4 + 2;
				indices[(i + TILES * j) * 6 + 3] = (i + TILES * j) * 4 + 0;
				indices[(i + TILES * j) * 6 + 4] = (i + TILES * j) * 4 + 2;
				indices[(i + TILES * j) * 6 + 5] = (i + TILES * j) * 4 + 3;
				
				for(int k = 0; k < v.length; k++)
					vertices[(i + TILES * j) * 4 + k] = v[k];
			}
		}
		
		setRenderer(new Renderer(new Mesh(vertices, indices)));
		transform.setPosition(100 + x * SIZE, 100 + y * SIZE);
	}
	
	public Chunk updateTexcoord(int x, int y) {
		VertexTex[] v = tiles[y][x].getVertices();
		for (int i = 0; i < v.length; i++)
			vertices[(x + TILES * y) + i] = v[i]; 
		setRenderer(new Renderer(new Mesh(vertices, indices)));
		return this;
	}
	
	public Chunk setTile(int x, int y, Tile tile) {
		tiles[y][x] = tile;
		updateTexcoord(x, y);
		return this;
	}
}