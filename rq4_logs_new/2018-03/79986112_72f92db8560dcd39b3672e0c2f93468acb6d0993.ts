import Resource = require("../../../src/Resource");
import Actor = require("../../../src/Actor");
import AttrId = require("../../../src/AttrId");
import Volume = require("../../../src/Volume");
import Collider = require("../../../src/Collider");

const ASA_PJ_NAME = "pj_stop_motion";

class HeartItem {
	public sprite: g.Sprite;
	public localXScale: number;
	public localYScale: number;
	private collisions: {[name: string]: Volume} = {};

	constructor(sprite: g.Sprite, localXScale: number, localYScale: number) {
		this.sprite = sprite;
		this.localXScale = localXScale;
		this.localYScale = localYScale;
	}

	addCollisions(colliders: Collider[]): void {
		this.collisions = {};
		colliders.forEach((collider) => {
			const volume = collider.getVolume();
			if (! volume) return;
			const aabb = volume.aabb();
			const isCollided = g.Collision.intersect(
				aabb.origin.x,
				aabb.origin.y,
				aabb.extent.width,
				aabb.extent.height,
				this.sprite.x,
				this.sprite.y,
				this.sprite.width,
				this.sprite.height
			);
			if (isCollided) {
				this.collisions[collider.name] = volume;
			}
		});
	}

	isCollided(name: string): boolean {
		return name in this.collisions;
	}
}

class DemoScene extends g.Scene {
	private actor: Actor = undefined;
	private heartItems: {[key: string]: HeartItem} = {};

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
			x: 160,
			y: 180,
			playSpeed: 1.0
		});
		this.append(this.actor);

		this.heartItems["x++"] = this.generateHeartItem("x_plus", 0, 0, 0.5, 0);
		this.heartItems["y++"] = this.generateHeartItem("y_plus", 64, 0, 0, 0.5);
		this.heartItems["x--"] = this.generateHeartItem("x_minus", 128, 0, -0.5, 0);
		this.heartItems["y--"] = this.generateHeartItem("y_minus", 192, 0, 0, -0.5);
		Object.keys(this.heartItems).forEach((key) => {
			const heartItem = this.heartItems[key];
			heartItem.sprite.pointMove.add((ev) => {
				const nextX = heartItem.sprite.x + ev.prevDelta.x;
				const nextY = heartItem.sprite.y + ev.prevDelta.y;
				if (
					0 <= nextX
					&& nextX + heartItem.sprite.width <= g.game.width
					&& 0 <= nextY
					&& nextY + heartItem.sprite.height <= g.game.height
				) {
					heartItem.sprite.x = nextX;
					heartItem.sprite.y = nextY;
					heartItem.sprite.modified();
					heartItem.addCollisions(this.actor.colliders);
				}
			});
			this.append(heartItem.sprite);
		});

		const messageLabel = new g.SystemLabel({
			text: "ハートを棒人間にくっつけてみよう",
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
				let isCollided = false;
				Object.keys(this.heartItems).forEach((key) => {
					const heartItem = this.heartItems[key];
					if (heartItem.isCollided(c.name)) {
						this.registerLocalScaleHandler(c.name, heartItem.localXScale, heartItem.localYScale);
						isCollided = true;
					}
				});
				if (! isCollided) {
					this.unregisterLocalScaleHandler(c.name);
				}
			});
			this.actor.modified();
			this.actor.calc();
		});
	}

	private registerLocalScaleHandler(partsName: string, localXScale: number, localYScale: number): void {
		// 指定したパーツにハンドラが存在しない場合のみ、ハンドラの登録を行う
		this.actor.calculated(partsName, true).addOnce((param) => {
			if (param.posture) {
				const t = param.currentFrame / param.frameCount;
				param.posture.attrs[AttrId.lsx] += localXScale * t;
				param.posture.attrs[AttrId.lsy] += localYScale * t;
				param.posture.updateMatrix();
			}
		});
	}

	private unregisterLocalScaleHandler(partsName: string): void {
		// 指定したパーツにハンドラが登録されている場合、登録されているハンドラの登録解除を行う
		const trigger = this.actor.calculated(partsName, false);
		if (trigger) {
			trigger.removeAll();
		}
	}

	private generateHeartItem(assetId: string, x: number, y: number, localXScale: number, localYScale: number): HeartItem {
		const sprite = new g.Sprite({
			scene: this,
			src: this.assets[assetId] as g.ImageAsset,
			x: x,
			y: y,
			width: 40,
			height: 40,
			srcWidth: 207,
			srcHeight: 167,
			opacity: 0.6,
			touchable: true
		});
		return new HeartItem(sprite, localXScale, localYScale);
	}
}

export = (param: g.GameMainParameterObject): void => {
	const scene = new DemoScene({
		game: g.game,
		assetIds: [
			"stickman",
			"x_minus",
			"x_plus",
			"y_minus",
			"y_plus",
			"an_stop_motion",
			"bn_stop_motion",
			"pj_stop_motion",
			"sk_stickman"
		]
	});
	g.game.pushScene(scene);
};