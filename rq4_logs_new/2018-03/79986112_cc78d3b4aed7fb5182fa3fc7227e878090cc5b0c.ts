import Resource = require("../../../src/Resource");
import Actor = require("../../../src/Actor");
import AttrId = require("../../../src/AttrId");

const ASA_PJ_NAME = "pj_support_image_inverted";

type Direction = "right" | "left";

class Runner {
	actor: Actor;
	collisionRect: g.FilledRect;
	direction: Direction;
	isInvertedStar: boolean = false;
	isInvertedHead: boolean = false;

	constructor(actor: Actor, collisionRect: g.FilledRect, isRightDirection: boolean) {
		this.actor = actor;
		this.collisionRect = collisionRect;
		this.direction = isRightDirection ? "right" : "left";
	}
}

class DemoScene extends g.Scene {
	private runners: Runner[] = [];

	constructor(param: g.SceneParameterObject) {
		super(param);
		this.loaded.add(this.onLoaded, this);
	}

	private generateRunner(resource: Resource, x: number, y: number, isRightDirection: boolean): Runner {
		const actor = new Actor({
			scene: this,
			resource: resource,
			animationName: "anime_1",
			skinNames: ["support_image_inverted"],
			boneSetName: "support_image_inverted",
			width: 240,
			height: 240,
			scaleX: 0.75,
			scaleY: 0.75,
			x: x,
			y: y,
			playSpeed: 1
		});
		const collisionRect =  new g.FilledRect({
			scene: this,
			cssColor: "black",
			width: actor.width,
			height: actor.height,
			x: x - actor.width / 3,
			y: y - actor.height / 3,
			opacity: 0.25,
			touchable: true
		});
		return new Runner(actor, collisionRect, isRightDirection);
	}

	private isCollidedByRunner(runner: Runner, name: string, x: number, y: number): boolean {
		const colliders = runner.actor.colliders.filter(c => c.getVolume() != null && c.name === name);
		if (colliders.length === 0) {
			return false;
		}
		const aabb = colliders[0].getVolume().aabb();
		return g.Collision.intersect(
			aabb.origin.x - aabb.extent.width / 2,
			aabb.origin.y - aabb.extent.height / 2,
			aabb.extent.width,
			aabb.extent.height,
			x,
			y,
			10,
			10
		);
	}

	private onLoaded(): void {
		const resource = new Resource();
		resource.loadProject(ASA_PJ_NAME, this.assets, g.game.assets);

		// メッセージ生成
		const message = new g.SystemLabel({
			text: "顔か☆をクリックしてみよう",
			fontSize: 28,
			textAlign: g.TextAlign.Center,
			textBaseline: g.TextBaseline.Alphabetic,
			textColor: "black",
			fontFamily: g.FontFamily.SansSerif,
			x: g.game.width / 2 | 0,
			y: g.game.height * 4 / 5 | 0,
			scene: this
		});
		this.append(message);

		// ランナー生成
		this.runners.push(this.generateRunner(resource, 100, 100, true));
		this.runners.push(this.generateRunner(resource, 350, 100, false));
		this.runners.forEach((runner) => {
			// ランナーの向き先変更イベントもここで登録する
			runner.actor.calculated("root", true).add((param) => {
				if (param.posture) {
					param.posture.attrs[AttrId.sx] *= runner.direction === "right" ? -1 : 1;
					param.posture.updateMatrix();
				}
			});
			runner.actor.calculated("stick_head", true).add((param) => {
				if (param.posture) {
					param.posture.attrs[AttrId.iflh] = runner.isInvertedHead;
					param.posture.updateMatrix();
				}
			});
			runner.actor.calculated("star", true).add((param) => {
				if (param.posture) {
					param.posture.attrs[AttrId.iflv] = runner.isInvertedStar;
					param.posture.updateMatrix();
				}
			});
			runner.collisionRect.pointDown.add((ev) => {
				const px = runner.collisionRect.x + ev.point.x;
				const py = runner.collisionRect.y + ev.point.y;
				if (this.isCollidedByRunner(runner, "stick_head", px, py)) {
					this.runners.forEach((r) => {
						r.isInvertedHead = !r.isInvertedHead;
					});
				}
				if (this.isCollidedByRunner(runner, "star", px, py)) {
					this.runners.forEach((r) => {
						r.isInvertedStar = !r.isInvertedStar;
					});
				}
			});
			this.append(runner.actor);
			this.append(runner.collisionRect);
		});

		this.update.add(() => {
			this.runners.forEach((runner) => {
				runner.actor.modified();
				runner.actor.calc();
			});
		});
	}
}

export = (param: g.GameMainParameterObject): void => {
	const scene = new DemoScene({
		game: g.game,
		assetIds: [
			"support_image_inverted",
			"an_anime_1",
			"bn_support_image_inverted",
			"pj_support_image_inverted",
			"sk_support_image_inverted"
		]
	});
	g.game.pushScene(scene);
};