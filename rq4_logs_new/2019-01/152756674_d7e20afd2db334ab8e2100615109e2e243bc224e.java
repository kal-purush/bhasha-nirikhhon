package com.adventuresof.screens;

import com.adventuresof.game.character.CharacterRecord;
import com.adventuresof.game.world.GameProgress;
import com.badlogic.gdx.Gdx;
import com.badlogic.gdx.Screen;
import com.badlogic.gdx.graphics.Color;
import com.badlogic.gdx.graphics.GL20;
import com.badlogic.gdx.scenes.scene2d.Actor;
import com.badlogic.gdx.scenes.scene2d.InputEvent;
import com.badlogic.gdx.scenes.scene2d.Stage;
import com.badlogic.gdx.scenes.scene2d.ui.Image;
import com.badlogic.gdx.scenes.scene2d.ui.Label;
import com.badlogic.gdx.scenes.scene2d.ui.TextButton;
import com.badlogic.gdx.scenes.scene2d.utils.ChangeListener;
import com.badlogic.gdx.scenes.scene2d.utils.ClickListener;
import com.badlogic.gdx.utils.viewport.FitViewport;
import com.mygdx.game.AdventuresOfGame;

public class CharacterSelectionScreen extends MainMenuScreen implements Screen {

	protected AdventuresOfGame parent;
	private Stage uiStage;
//	private Skin skin;
	private TextButton startGame;
	private Label titleLabel;
	private TextButton nextButton;
	private TextButton previousButton;
	private Image elfSprite;

	public CharacterSelectionScreen(AdventuresOfGame _game) {
		parent = _game;
		FitViewport viewport = new FitViewport(160, 120);
		uiStage = new Stage(viewport);
		TextButton.TextButtonStyle buttonStyle = new TextButton.TextButtonStyle();
		Label.LabelStyle labelStyle = new Label.LabelStyle();

		buttonStyle.font = parent.res.gamefont;
		buttonStyle.fontColor = Color.WHITE;
		labelStyle.font = parent.res.gamefont;
		labelStyle.fontColor = Color.WHITE;
		titleLabel = new Label("The Adventures Of ", labelStyle);
		startGame = new TextButton("Start", buttonStyle);
		nextButton = new TextButton(">>>", buttonStyle);
		previousButton = new TextButton("<<<", buttonStyle);
		uiStage.act(Gdx.graphics.getDeltaTime());
		uiStage.draw();
		Gdx.input.setInputProcessor(uiStage);
		addListeners();
	}

	@Override
	public void show() {

		titleLabel.setFontScale(0.50f, 0.50f);
		titleLabel.setPosition((uiStage.getWidth() * 5 / 3 - titleLabel.getWidth()) / 2, uiStage.getHeight() * 5 / 6);
		uiStage.addActor(titleLabel);
		
		startGame.setPosition((uiStage.getWidth() - startGame.getWidth()) / 2, uiStage.getHeight() / 6);
		uiStage.addActor(startGame);

		elfSprite = new Image(
				parent.res.playerSprites.get(CharacterRecord.CHARACTERS[GameProgress.currentCharacter].name));
		elfSprite.setPosition((uiStage.getWidth() - elfSprite.getWidth()) / 2,
				(uiStage.getHeight() - elfSprite.getHeight()) / 2);
		uiStage.addActor(elfSprite);

		nextButton.setPosition(uiStage.getWidth() * 5 / 6 - nextButton.getWidth() / 2, uiStage.getHeight() * 3 / 6);
		uiStage.addActor(nextButton);

		previousButton.setPosition(uiStage.getWidth() / 6, uiStage.getHeight() * 3 / 6);
		uiStage.addActor(previousButton);
	}

	@Override
	public void render(float delta) {
		Gdx.gl.glClearColor(0, 0, 0, 1);
		Gdx.gl.glClear(GL20.GL_COLOR_BUFFER_BIT);
		uiStage.act(delta);
		uiStage.draw();
	}

	@Override
	public void resize(int width, int height) {
		uiStage.getViewport().update(width, height, true);
	}

	public void addListeners() {
		startGame.addListener(new ChangeListener() {
			@Override
			public void changed(ChangeEvent event, Actor actor) {
				parent.changeScreen(ScreenType.MAINGAMESCREEN);
			}
		});

		nextButton.addListener(new ClickListener() {
			@Override
			public void touchUp(InputEvent event, float x, float y, int pointer, int button) {
				GameProgress.currentCharacter += 1;
				if (GameProgress.currentCharacter == CharacterRecord.CHARACTERS.length) {
					GameProgress.currentCharacter = 0;
				}
				uiStage.clear();
				show();
				addListeners();
			}
		});
		previousButton.addListener(new ClickListener() {
			@Override
			public void touchUp(InputEvent event, float x, float y, int pointer, int button) {
				GameProgress.currentCharacter -= 1;
				if (GameProgress.currentCharacter < 0) {
					GameProgress.currentCharacter = CharacterRecord.CHARACTERS.length - 1;
				}
				uiStage.clear();
				show();
				addListeners();
			}
		});
	}

	@Override
	public void dispose() {
		uiStage.dispose();
	}

	@Override
	public void pause() {
		// TODO Auto-generated method stub

	}

	@Override
	public void resume() {
		// TODO Auto-generated method stub

	}

	@Override
	public void hide() {
		// TODO Auto-generated method stub

	}

}