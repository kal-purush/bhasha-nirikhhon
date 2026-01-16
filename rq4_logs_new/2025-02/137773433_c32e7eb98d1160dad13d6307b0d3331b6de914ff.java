package com.jcoadyschaebitz.neon.cutscene;

import com.jcoadyschaebitz.neon.cutscene.events.CameraMoveEvent;
import com.jcoadyschaebitz.neon.cutscene.events.CameraMoveEvent.Transition;
import com.jcoadyschaebitz.neon.cutscene.events.EndSceneEvent;
import com.jcoadyschaebitz.neon.cutscene.events.SmoothCameraMoveEvent;
import com.jcoadyschaebitz.neon.cutscene.events.SpeakEvent;
import com.jcoadyschaebitz.neon.entity.mob.Mob;
import com.jcoadyschaebitz.neon.graphics.Sprite;
import com.jcoadyschaebitz.neon.input.InputManager;
import com.jcoadyschaebitz.neon.level.Level;
import com.jcoadyschaebitz.neon.util.Vec2i;

public class Scene_0_Train extends CutScene {

	public Scene_0_Train(Level level, Mob[] members, InputManager input) {
		super(new Vec2i(30, 104), new Vec2i(36, 132), 16 * 16, 8 * 16, level, members, input);
	}

	@Override
	protected void initEvents() {
		actors[0] = new Actor(level.getPlayer());
		addEvent(new CameraMoveEvent(this, 60, 540, currentScreenX, currentScreenY + 18 * 16));
		addEvent(new SpeakEvent(this, 600, 240, "-- text from old boss giving dying instructions, recording? --", Sprite.car_down));
		addEvent(new SmoothCameraMoveEvent(this, 840, 180, Transition.OUT));
		addEvent(new EndSceneEvent(this, 1050, 0));
	}

}