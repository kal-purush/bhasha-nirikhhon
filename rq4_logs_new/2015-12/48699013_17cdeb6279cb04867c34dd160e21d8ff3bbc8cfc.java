package xsked.level;

import time.api.debug.Debug;
import time.api.entity.Entity;
import time.api.gfx.QuadRenderer;
import time.api.gfx.texture.Texture;
import time.api.math.Vector2f;
import time.api.physics.Body;

public class Spell extends Entity {
	
	public static final float MAX_HEALTH = 3f;
	
	public static final float SIZE = Tile.SIZE / 2;
	
	public final SpellType TYPE;
	
	private Level level;
	
	private float speed;
	
	private Vector2f dir;
	
	private float health = MAX_HEALTH;
	
	public Spell(Level level, float x, float  y, SpellType type, float speed, Vector2f dir) {
		this.level = level;
		this.TYPE = type;
		this.speed = speed;
		this.dir = dir;
		
		setRenderer(new QuadRenderer(x, y, SIZE, SIZE, Texture.get("element_" + type.name().toLowerCase())));
		body = new Body(transform, SIZE, SIZE).setTrigger(true).setAbsolute(true);
		body.addTag(type.name());
		level.getPhysicsEngien().addBody(body);
	}
	
	public void update(float delta) {
		body.getPos().add(dir.clone().scale(speed * delta));
		health -= delta;
		
		
		
		if(health <= 0) {
			renderer.getMesh().destroy();
			Debug.log("TODO: remove spell bodies from physics engine");
			level.removeSpell(this);
		}
	}
	
	public enum SpellType {
		EARTH("spell_earth"),
		WIND("spell_wind"),
		WATER("spell_water"),
		FIRE("spell_fire");
		
		public final String TEXTURE_NAME;
		
		SpellType(String textureName) {
			this.TEXTURE_NAME = textureName;
		}
	}
}