"use strict";

let ECS = require('yagl-ecs'),
	Sprite = require('../../engine/component/sprite'),
	Skybox = require('../../engine/component/skybox'),
	Position = require('../../engine/component/position'),
	Vector2 = require('gl-matrix').vec2
;

class Background extends ECS.Entity {

	constructor() {

		super( null, [
			Sprite,
			Position,
			Skybox
		] );

		this.updateComponent('sprite', {
			src: 'assets/textures/bg.jpg',
			width: 3133,
			height: 2374,
			anchor: Vector2.fromValues(0, 0)
		});
	}
}

module.exports = Background;