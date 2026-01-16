import Resource = require("../../../src/Resource");
import Actor = require("../../../src/Actor");
import AttrId = require("../../../src/AttrId");

const ASA_PJ_NAME = "pj_stop_motion";

class DemoScene extends g.Scene {
	private actor: Actor;

	constructor(param: g.SceneParameterObject) {
		super(param);
		this.loaded.add(this.onLoaded, this);
	}

	private onLoaded(): void {
		const resource = new Resource();
		resource.loadProject(ASA_PJ_NAME, this.assets, g.game.assets);
		this.actor = new Actor({
			scene: this,
			resource: resource,
			animationName: "stop_motion",
			skinNames: ["stickman"],
			boneSetName: "stop_motion",
			width: 320,
			height: 320,
			x: 200,
			y: 200,
			playSpeed: 1.0
		});
		this.append(this.actor);

		const sprite = new g.Sprite({
			scene: this,
			src: this.assets["shine"] as g.ImageAsset,
			x: 220,
			y: 0,
			width: 40,
			height: 40,
			srcWidth: 115,
			srcHeight: 115,
			opacity: 0.6,
			touchable: true
		});
		sprite.pointMove.add((ev) => {
			const nextX = sprite.x + ev.prevDelta.x;
			const nextY = sprite.y + ev.prevDelta.y;
			if (
				0 <= nextX
				&& nextX + sprite.width <= g.game.width
				&& 0 <= nextY
				&& nextY + sprite.height <= g.game.height
			) {
				sprite.x = nextX;
				sprite.y = nextY;
				sprite.modified();
			}
		});
		this.append(sprite);

		const messageLabel = new g.SystemLabel({
			text: "太陽を人型物体にくっつけてみよう",
			fontSize: 18,
			textAlign: g.TextAlign.Center,
			textBaseline: g.TextBaseline.Alphabetic,
			textColor: "black",
			fontFamily: g.FontFamily.SansSerif,
			strokeWidth: 0.25,
			x: g.game.width / 2 | 0,
			y: g.game.height * 9 / 10 | 0,
			scene: this
		});
		this.append(messageLabel);

		this.update.add(() => {
			this.actor.colliders.forEach((c) => {
				const volume = c.getVolume();
				if (! volume) return;
				const aabb = volume.aabb();
				if (g.Collision.intersect(
					aabb.origin.x,
					aabb.origin.y,
					aabb.extent.width,
					aabb.extent.height,
					sprite.x,
					sprite.y,
					sprite.width,
					sprite.height
				)) {
					this.registerLocalAlphaHandler(c.name);
				} else {
					this.unregisterLocalAlphaHandler(c.name);
				}
			});
			this.actor.modified();
			this.actor.calc();
		});
	}

	private registerLocalAlphaHandler(partsName: string): void {
		// 指定したパーツにハンドラが存在しない場合のみ、ハンドラの登録を行う
		this.actor.calculated(partsName, true).addOnce((param) => {
			if (param.posture) {
				const t = param.currentFrame / param.frameCount;
				param.posture.attrs[AttrId.lalpha] = 1 - t;
				param.posture.updateMatrix();
			}
		});
	}

	private unregisterLocalAlphaHandler(partsName: string): void {
		// 指定したパーツにハンドラが登録されている場合、登録されているハンドラの登録解除を行う
		const trigger = this.actor.calculated(partsName, false);
		if (trigger) {
			trigger.removeAll();
		}
	}
}

export = (param: g.GameMainParameterObject): void => {
	const scene = new DemoScene({
		game: g.game,
		assetIds: [
			"stickman",
			"shine",
			"an_stop_motion",
			"bn_stop_motion",
			"pj_stop_motion",
			"sk_stickman"
		]
	});
	g.game.pushScene(scene);
};