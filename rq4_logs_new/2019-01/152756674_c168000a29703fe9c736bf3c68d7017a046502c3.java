package com.adventuresof.game.quest;

import com.adventuresof.game.character.Player;
import com.badlogic.gdx.Gdx;
import com.badlogic.gdx.Screen;
import com.badlogic.gdx.graphics.GL20;
import com.badlogic.gdx.graphics.g2d.SpriteBatch;
import com.badlogic.gdx.graphics.g2d.TextureAtlas;
import com.badlogic.gdx.scenes.scene2d.Actor;
import com.badlogic.gdx.scenes.scene2d.Stage;
import com.badlogic.gdx.scenes.scene2d.ui.List;
import com.badlogic.gdx.scenes.scene2d.ui.ScrollPane;
import com.badlogic.gdx.scenes.scene2d.ui.Skin;
import com.badlogic.gdx.scenes.scene2d.ui.Table;
import com.badlogic.gdx.scenes.scene2d.ui.TextArea;
import com.badlogic.gdx.scenes.scene2d.ui.Window;
import com.badlogic.gdx.scenes.scene2d.utils.ChangeListener;
import com.badlogic.gdx.scenes.scene2d.utils.ChangeListener.ChangeEvent;
import com.badlogic.gdx.utils.viewport.StretchViewport;

public class QuestInfoActor extends Window {
	
	ScrollPane scrollPane;
    List<String> list;
    SpriteBatch batcher;
    float gameWidth, gameHeight;
    private Stage stage;
	private String title;
	private String description;
	private java.util.List<Task> tasks;
	private TextArea textArea;
	private Player player;
	private Skin skin;
 
    public QuestInfoActor(Skin skin, Player player) {
    	super("Quest", skin);
    	
    	this.player = player;
    	this.skin = skin;
    	gameWidth = Gdx.graphics.getWidth();
        gameHeight = Gdx.graphics.getHeight();
    	   	
    }
    
    public void setQuestInfo(String selectedQuestTitle) {
    	for (Quest quest : player.getQuests()) { 
			
			if(quest.getTitle().equals(selectedQuestTitle)) {
//			if(selectedQuestTitle.equals("String: 1")) {

				this.title = quest.getTitle();
				this.description = quest.getDescription();
				this.tasks = quest.getTasks();
				
//				this.textArea = new TextArea("", skin);
//				this.textArea.setDisabled(true);
//				this.textArea.setText("");
//				this.textArea.setWidth(800);
//				this.textArea.setHeight(120);
				
//				textArea.setText(this.title);
				clear();
//				add(this.title);
				
				Table uiTable = new Table(skin);
				uiTable.setFillParent(true);
				uiTable.pad(10);
				uiTable.add(this.title).bottom().colspan(3);
				uiTable.row();
				uiTable.add(this.description).expandX().expandY();
				uiTable.row();
				uiTable.add().bottom();
				
				int i = 0;
				
				for (Task task : this.tasks) {
					
					i++;
					
					String taskDescription = task.getDescription() == null ? "" : " - " + task.getDescription();
					
					uiTable.row();
					uiTable.add("Step " + i + ":" + task.getTitle() + taskDescription);
				}
				
				add(uiTable);
				
				//add(textArea);
				
		        pack();
				
		        setVisible(true);
			}
			
		}
    }

}

