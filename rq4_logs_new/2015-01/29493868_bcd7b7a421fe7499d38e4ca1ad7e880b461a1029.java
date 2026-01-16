package com.makzk.games.entities;

import static com.makzk.games.util.PlayerAnimations.ANIMATION_FALL;
import static com.makzk.games.util.PlayerAnimations.ANIMATION_RUN;
import static com.makzk.games.util.PlayerAnimations.ANIMATION_STAND;

import org.newdawn.slick.GameContainer;
import org.newdawn.slick.SlickException;
import org.newdawn.slick.SpriteSheet;
import org.newdawn.slick.geom.Rectangle;

import com.makzk.games.util.Direction;

public class Enemy extends EntityRect {

	public Enemy(GameContainer gc, Rectangle rect) throws SlickException {
		super(gc, rect);
		gravity = true;
		solid = true;

		// Configurar animaciones
		SpriteSheet sprite = new SpriteSheet("data/sprites/bugs_walk.png", 43, 82);
		setupAnimation(sprite, ANIMATION_STAND, new int[]{0,1,2,3,4,5}, 200);
		setupAnimation(sprite, ANIMATION_RUN, 0, 0, 4, 0, 200);
		setupAnimation(sprite, ANIMATION_FALL, new int[]{0}, 200);
		
		speedX = .2f;
	}

	@Override
	public void onCollision(Direction dir, EntityRect other) {
		if(dir == Direction.WEST || dir == Direction.EAST) {
			speedX = -speedX;
		}
	}
}