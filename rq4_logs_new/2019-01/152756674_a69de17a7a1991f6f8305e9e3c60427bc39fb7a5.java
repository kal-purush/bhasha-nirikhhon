package com.adventuresof.screens;

import com.adventuresof.helpers.SoundManager;
import com.badlogic.gdx.Gdx;
import com.badlogic.gdx.Screen;
import com.badlogic.gdx.graphics.GL20;
import com.badlogic.gdx.scenes.scene2d.Actor;
import com.badlogic.gdx.scenes.scene2d.Stage;
import com.badlogic.gdx.scenes.scene2d.ui.Label;
import com.badlogic.gdx.scenes.scene2d.ui.Skin;
import com.badlogic.gdx.scenes.scene2d.ui.Table;
import com.badlogic.gdx.scenes.scene2d.ui.TextButton;
import com.badlogic.gdx.scenes.scene2d.utils.ChangeListener;
import com.badlogic.gdx.utils.viewport.ScreenViewport;
import com.mygdx.game.AdventuresOfGame;

public class CreditScreen implements Screen{

	protected AdventuresOfGame parent;
	public Stage stage;
	private Skin skin;
	private Label titleLabel;
	private Label line1;
	private Label line2;
	private Label line3;
	private Label line4;
	private Label line5;
	private Label line6;
	private Label line7;
	private Label line8;
	private Label line9;
	private Label line10;
	private Label line11;
	private Label line12;
	private Label line13;
	private Label line14;

	private TextButton back;

	
	public CreditScreen(AdventuresOfGame game) {
		parent = game;
		stage = new Stage(new ScreenViewport());
		skin = new Skin(Gdx.files.internal("skins/star-soldier-ui.json"));
		titleLabel = new Label("The Adventures Of ...", skin);
		line1 = new Label("== Credits ==", skin);
		line2 = new Label("Graphics:", skin);
		line3 = new Label("Item Artwork: Ravenmore - http://dycha.net", skin);
		line4 = new Label("Spell Animation Artwork: Daniel Eddeland -" + "\nhttps://opengameart.org/content/extended-lpc-magic-pack", skin);
		line5 = new Label("Dragon Character and Animations: Created by ZaPaper & Jordan Irwin (AntumDeluge);" + "\nCredits to http://www.buko-studios.com/" + "\nCommissioned by PlayCraft:https://www.playcraftapp.com/", skin);
		line6 = new Label("Other Character Credits: Sithjester's RMXP Resources -" + "\nhttp://untamed.wild-refuge.net/rmxpresources.php?characters", skin);
		line7 = new Label("Tilesets By: Finalbossblues - http://finalbossblues.com/timefantasy/\n", skin);
		line8 = new Label("Arrow Animations: Clint Bellanger - https://opengameart.org/content/projectiles", skin);
		line9 = new Label("Music:", skin);
		line10 = new Label("Wasteland Showdown: Matthew Pablo -" + "\nhttps://opengameart.org/content/wasteland-showdown-battle-music", skin);
		line11 = new Label("Soliloquy: Matthew Pablo - https://opengameart.org/content/soliloquy", skin);
		line12 = new Label("Heroic Demise: Matthew Pablo - https://opengameart.org/content/heroic-demise-updated-version", skin);
		line13 = new Label("No More Magic: HorrorPen - https://opengameart.org/content/no-more-magic", skin);
		line14 = new Label("Spell Icons: J. W. Bjerk (eleazzaar) -- www.jwbjerk.com/art --" + "\nfind this and other open art at: http://opengameart.org", skin);
		
		back = new TextButton("Back", skin);
		stage.act(Gdx.graphics.getDeltaTime());
		stage.draw();
		Gdx.input.setInputProcessor(stage);
		addListeners();
		
		// Music setup
		SoundManager.playMusic("audio/music/soliloquy.mp3");
	}
	
	@Override
	public void show() {
		titleLabel.setFontScale(1.5f, 1.5f);
		Table table = new Table();
		table.setFillParent(true);
		stage.addActor(table);
		table.add(titleLabel);
		table.row().pad(30,0,0,10);
		table.add(line1);
		table.row().pad(10,0,0,10);
		table.add(line2);
		table.row();
		table.add(line3);
		table.row();
		table.add(line4);
		table.row();
		table.add(line5);
		table.row();
		table.add(line6);
		table.row();
		table.add(line7);
		table.row();
		table.add(line8);
		table.row().pad(10,0,0,10);
		table.add(line9);
		table.row();
		table.add(line10);
		table.row();
		table.add(line11);
		table.row();
		table.add(line12);
		table.row();
		table.add(line13);
		table.row();
		table.add(line14);
		table.row();
		table.add(back).fillX().uniformX(); 
	}

	@Override
	public void render(float delta) {
		Gdx.gl.glClearColor(0f, 0f, 0f, 1);
		Gdx.gl.glClear(GL20.GL_COLOR_BUFFER_BIT);
		stage.act(Math.min(Gdx.graphics.getDeltaTime(), 1 / 30f));
		stage.draw();
	}

	@Override
	public void resize(int width, int height) {		
		stage.getViewport().update(width, height, true);
	}
	
	public void addListeners() {	
		back.addListener(new ChangeListener() {
			@Override
			public void changed(ChangeEvent event, Actor actor) {
				parent.changeScreen(ScreenType.MAINMENU);
			}
		});
	}

	@Override
	public void pause() {		
	}

	@Override
	public void resume() {		
	}

	@Override
	public void hide() {		
	}

	@Override
	public void dispose() {	
		stage.dispose();
	}

}