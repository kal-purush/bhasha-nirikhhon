package com.codygordon.game.demos.gravity;

import java.awt.Color;

import com.codygordon.game.Game;
import com.codygordon.game.input.EventListener;
import com.codygordon.game.input.events.KeyDownEvent;
import com.codygordon.game.ui.GameView;

public class TestView extends GameView implements EventListener {

	@Override 
	public void onEnable() {
		setBackground(Color.BLUE);
		Game.getInstance().registerEventListener(this);
	}

	@Override
	public void onKeyPressed(KeyDownEvent event) {
		Game.getInstance().switchScreen(new GravityDemoView());
	}
	
	@Override
	public void onDisable() {
		Game.getInstance().unRegisterEventListener(this);
	}
}