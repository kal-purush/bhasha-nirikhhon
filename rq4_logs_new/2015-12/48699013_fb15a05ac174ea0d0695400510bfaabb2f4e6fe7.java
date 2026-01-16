package xsked.level;

import time.api.entity.Entity;
import time.api.gfx.QuadRenderer;
import time.api.gfx.texture.Texture;
import time.api.math.Vector2f;
import time.api.physics.Body;

public class Ghost extends Entity{
	
	public static final float GHOST_SPEED = 10.0f;
	
	private Level level;
	private Player player;
	private Tag weakness;
	private boolean isDead;
	
	public Ghost(Level level, float x, float y) {
		super();
		this.level = level;
		this.player = level.getPlayer();
		
		isDead = false;
		
		this.setRenderer(new QuadRenderer(x * Tile.SIZE, y * Tile.SIZE, Tile.SIZE, Tile.SIZE, Texture.DEFAULT_TEXTURE));
		Body b = new Body(transform, Tile.SIZE, Tile.SIZE).setTrigger(true);
		b.addTag(Tag.LEATHAL.name());
		this.setBody(b);
	}
	
	public void setWeakness(Tag tag) {
		weakness = tag;
	}
	
	public void update(float delta) {
		if (isDead) return;
		
		super.update(delta);
		
		if (body.isCollidingWith(weakness.name())) 
			die();
		
		Vector2f dp = new Vector2f(player.getX() - getX(), player.getY() - getY());
		dp.scale(((float) (1.0/dp.getMagnitude()) * GHOST_SPEED));
		body.setVel((Vector2f) body.getVel().scale(0.5f));
		body.addVel(dp);
	}
	
	private void die() {
		isDead = true;
		this.renderer = null;
		this.body.setPos(new Vector2f(0, -100));
	}
}	