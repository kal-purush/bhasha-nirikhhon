var Blocks = (function () {

	function Block(sprite, mesh) {
		this.sprite = sprite;
		this.mesh = mesh;
		mesh.position.x = sprite.x * Config.UNIT_LENGTH;
		mesh.position.y = sprite.y * Config.UNIT_LENGTH;
		mesh.position.z = sprite.z * Config.UNIT_LENGTH;
	}

	Block.prototype.rotate = function() {
		this.sprite.shape = Shapes.rotateY(this.sprite.shape);
		this.mesh.rotateY(Math.PI / 2);
	};

	Block.prototype.move = function(dx, dy, dz) {
		this.sprite.x += dx;
		this.sprite.y += dy;
		this.sprite.z += dz;
		
		this.mesh.position.x += dx * Config.UNIT_LENGTH;
		this.mesh.position.y += dy * Config.UNIT_LENGTH;
		this.mesh.position.z += dz * Config.UNIT_LENGTH;
		
	};


	return {
		Block: Block
	};

})();