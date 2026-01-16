package com.adventuresof.game.animation;

import com.adventuresof.helpers.AnimationFactory;
import com.badlogic.gdx.graphics.g2d.Animation;
import com.badlogic.gdx.graphics.g2d.TextureRegion;

public class StaticMapItemAnimation {

	// movement
	private Animation<TextureRegion> animation;

	// texture sheet
	protected String textureSheet;

	// texture sheet sizes
	int textureSheetCols, textureSheetRows;

	// movement values
	int moveStart, moveFrames;

	// movement, attack and death animations - for when all animations are on same sheet
	public StaticMapItemAnimation(String textureSheet, int sheetRows, int sheetCols, int moveStart, int moveFrames) 
	{	
		this.textureSheetRows = sheetRows;
		this.textureSheetCols = sheetCols;
		this.textureSheet = textureSheet;

		this.moveFrames = moveFrames;
		this.moveStart = moveStart;

		this.initializeAnimations();
	}


	public Animation<TextureRegion> getAnimation() {
		return animation;
	}


	public void setAnimation(Animation<TextureRegion> animation) {
		this.animation = animation;
	}


	private void initializeAnimations() {
		this.createMovementAnimations();
	}		

	private void createMovementAnimations() {
		this.setAnimation(AnimationFactory.createAnimation(this.textureSheet, this.textureSheetCols, this.textureSheetRows, this.moveFrames, this.moveStart, 0.08f));
	}
}

