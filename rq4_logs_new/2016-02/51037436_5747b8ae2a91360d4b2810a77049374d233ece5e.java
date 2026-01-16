package com.engine.core;

import org.lwjgl.opengl.Display;
import org.lwjgl.util.vector.Vector3f;

import com.engine.core.entity.Entity;
import com.engine.core.entity.Light;
import com.engine.core.models.RawModel;
import com.engine.core.models.TexturedModel;
import com.engine.core.render.MasterRenderer;
import com.engine.core.terrain.Terrain;
import com.engine.core.textures.ModelTexture;
import com.game.core.Player;

public class MainGame {
	
	private Loader loader;
	private MasterRenderer renderer;
	
	private RawModel model;
	private ModelTexture texture;
	private TexturedModel texturedModel;
	
	private Entity entity;
	private Light light;
	private Player player;;
	
	private Terrain terrain, terrain2;
	
	public MainGame() {
		Window.create();
	}
	
	private void init() {
		loader = new Loader();
		renderer = new MasterRenderer();
		model = loader.loadObjModel("stall.obj");
		texture = new ModelTexture(loader.loadTexture("stallTexture.png"));
		texturedModel = new TexturedModel(model, texture);
		entity = new Entity(texturedModel, new Vector3f(0,0,-25), new Vector3f(0,180,0), 1);
		light = new Light(new Vector3f(0,0,-20), new Vector3f(1,1,1));
		player = new Player();
		
		terrain = new Terrain(0,-1,loader,new ModelTexture(loader.loadTexture("grass.png")));
		terrain2 = new Terrain(-1,-1,loader,new ModelTexture(loader.loadTexture("grass.png")));
	}
	
	private void input() {
		player.input();
	}
	
	private void update() {
		player.update();
	}
	
	private void render() {
		renderer.render(light, player.getCamera());
		renderer.processEntity(entity);
		renderer.processTerrain(terrain);
		renderer.processTerrain(terrain2);
		Window.update();
	}
	
	private void gameLoop() {
		while(!Display.isCloseRequested()) {
			input();
			update();
			render();
		}
	}
	
	private void cleanUp() {
		loader.cleanUp();
		Window.close();
	}
	
	public void run() {
		init();
		gameLoop();
		cleanUp();
	}
}