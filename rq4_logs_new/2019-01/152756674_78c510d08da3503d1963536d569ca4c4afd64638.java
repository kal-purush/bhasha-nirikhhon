package com.adventuresof.game.quest;

import com.badlogic.gdx.Gdx;
import com.badlogic.gdx.Screen;
import com.badlogic.gdx.graphics.GL20;
import com.badlogic.gdx.graphics.g2d.SpriteBatch;
import com.badlogic.gdx.graphics.g2d.TextureAtlas;
import com.badlogic.gdx.scenes.scene2d.Stage;
import com.badlogic.gdx.scenes.scene2d.ui.List;
import com.badlogic.gdx.scenes.scene2d.ui.ScrollPane;
import com.badlogic.gdx.scenes.scene2d.ui.Skin;
import com.badlogic.gdx.scenes.scene2d.ui.Window;
import com.badlogic.gdx.utils.viewport.StretchViewport;

public class QuestLogActor extends Window {
	
	ScrollPane scrollPane;
    List<String> list;
    SpriteBatch batcher;
    float gameWidth, gameHeight;
    private Stage stage;
 
    public QuestLogActor(Skin skin) {
    	super("Quest Log", skin);

    	
    	batcher = new SpriteBatch();
        list = new List<String>(skin);
        String[] strings = new String[20];
        for (int i = 0, k = 0; i < 20; i++) {
            strings[k++] = "String: " + i;
 
        }
        list.setItems(strings);
        scrollPane = new ScrollPane(list);
        scrollPane.setBounds(0, 0, gameWidth, gameHeight + 100);
        scrollPane.setSmoothScrolling(false);
        scrollPane.setPosition(gameWidth / 2 - scrollPane.getWidth() / 4,
                gameHeight / 2 - scrollPane.getHeight() / 4);
        scrollPane.setTransform(true);
        scrollPane.setScale(0.5f);
        stage = new Stage(new StretchViewport(gameWidth, gameHeight));
        stage.addActor(scrollPane);
        Gdx.input.setInputProcessor(stage);
 
    }
 
    float backgroundX = 0;

}

