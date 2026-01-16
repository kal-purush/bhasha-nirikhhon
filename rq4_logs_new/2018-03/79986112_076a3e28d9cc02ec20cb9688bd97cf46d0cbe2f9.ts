import Resource = require("../../../src/Resource");
import Actor = require("../../../src/Actor");
import Bone = require("../../../src/Bone");
import AlphaBlendMode = require("../../../src/AlphaBlendMode");

const ASA_PJ_NAME = "pj_practice_swing";
const CSS_COLORS = [
	"white",
	"gray",
	"blue",
	"navy",
	"teal",
	"green",
	"lime",
	"aqua",
	"yellow",
	"fuchsia",
	"olive",
	"purple",
	"maroon"
];
const ALPHA_BLEND_TYPES: AlphaBlendMode[] = ["normal", "add"];

class DemoScene extends g.Scene {
	private actor: Actor = undefined;
	private bgRect: g.FilledRect = undefined;
	private bgColorIndex: number = 0;
	private colorButton: g.E = undefined;
	private alphaBlendIndex: number = 0;

	constructor(param: g.SceneParameterObject) {
		super(param);
		this.loaded.add(this.onLoaded, this);
	}

	private onLoaded(): void {
		const resource = new Resource();
		resource.loadProject(ASA_PJ_NAME, this.assets, g.game.assets);

		// 背景色
		this.bgRect = new g.FilledRect({
			scene: this,
			cssColor: CSS_COLORS[this.bgColorIndex],
			width: g.game.width,
			height: g.game.height,
			x: 0,
			y: 0,
			touchable: true
		});
		this.bgRect.pointDown.add(() => {
			this.bgColorIndex = (this.bgColorIndex + 1) % CSS_COLORS.length;
			this.bgRect.cssColor = CSS_COLORS[this.bgColorIndex];
		});
		this.append(this.bgRect);

		// Actor
		this.actor = new Actor({
			scene: this,
			resource: resource,
			animationName: "anime_1",
			skinNames: ["stickman"],
			boneSetName: "stickman",
			width: 320,
			height: 320,
			x: 200,
			y: 180,
			playSpeed: 1.0
		});
		this.append(this.actor);

		// ボタン
		this.colorButton = this.generateColorButton(g.game.width * 3 / 10, g.game.height * 7.5 / 10);
		this.append(this.colorButton);

		const messageLabel = new g.SystemLabel({
			text: "ボタンを押すと剣、背景を押すと背景の色が変わります",
			fontSize: 16,
			textAlign: g.TextAlign.Center,
			textBaseline: g.TextBaseline.Alphabetic,
			textColor: "black",
			fontFamily: g.FontFamily.SansSerif,
			strokeWidth: 0.25,
			x: g.game.width / 2 | 0,
			y: g.game.height * 9.5 / 10 | 0,
			scene: this
		});
		this.append(messageLabel);

		this.update.add(() => {
			this.actor.modified();
			this.actor.calc();
		});
	}

	private generateColorButton(x: number, y: number): g.E {
		const button = new g.E({
			scene: this,
			x: x,
			y: y
		});
		const buttonSprite = this.generateButtonSprite("button");
		button.append(buttonSprite);
		const pushedButtonSprite = this.generateButtonSprite("pushed_button");
		pushedButtonSprite.hide();
		button.append(pushedButtonSprite);
		const buttonLabel = new g.SystemLabel({
			text: this.getButtonText(ALPHA_BLEND_TYPES[this.alphaBlendIndex]),
			fontSize: 12,
			textAlign: g.TextAlign.Center,
			textColor: "white",
			fontFamily: g.FontFamily.SansSerif,
			x: buttonSprite.width / 2,
			y: buttonSprite.height / 2,
			scene: this
		});
		button.append(buttonLabel);
		const touchableRect = new g.FilledRect({
			scene: this,
			cssColor: "white",
			width: buttonSprite.width,
			height: buttonSprite.height,
			opacity: 0,
			touchable: true
		});
		touchableRect.pointDown.add(() => {
			buttonSprite.hide();
			pushedButtonSprite.show();
		});
		touchableRect.pointUp.add(() => {
			pushedButtonSprite.hide();
			buttonSprite.show();
			this.alphaBlendIndex = (this.alphaBlendIndex + 1) % ALPHA_BLEND_TYPES.length;
			buttonLabel.text = this.getButtonText(ALPHA_BLEND_TYPES[this.alphaBlendIndex]);
			this.getBone("dagger").alphaBlendMode = ALPHA_BLEND_TYPES[this.alphaBlendIndex];
		});
		button.append(touchableRect);
		return button;
	}

	private generateButtonSprite(assetId: string): g.Sprite {
		return new g.Sprite({
			scene: this,
			src: this.assets[assetId] as g.ImageAsset,
			width: 120,
			height: 60,
			srcWidth: 205,
			srcHeight: 111
		});
	}

	private getButtonText(alphaBlendMode: AlphaBlendMode): string {
		const buttonText = "α-blend：";
		switch (alphaBlendMode) {
			case "add":
				return buttonText + "加算";
			case "normal":
				return buttonText + "通常";
			default:
				return buttonText + "不明";
		}
	}

	private getBone(name: string): Bone {
		const targetBones = this.actor.skeleton.bones.filter(bone => name === bone.name);
		return targetBones[0];
	}
}

export = (param: g.GameMainParameterObject): void => {
	const scene = new DemoScene({
		game: g.game,
		assetIds: [
			"stickman",
			"button",
			"pushed_button",
			"an_anime_1",
			"bn_stickman",
			"sk_stickman",
			"pj_practice_swing"
		]
	});
	g.game.pushScene(scene);
};