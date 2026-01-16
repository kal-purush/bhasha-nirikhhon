class CameraStartFollowActionScript extends ScriptNode {
    constructor(parent: ScriptNode | Phaser.GameObjects.GameObject | Phaser.Scene);
    roundPixels: boolean;
    lerpX: number;
    lerpY: number;
    offsetX: number;
    offsetY: number;
    execute(...args: any[]): void;
}

class CameraStopFollowActionScript extends ScriptNode {
    constructor(parent: ScriptNode | Phaser.GameObjects.GameObject | Phaser.Scene);
    execute(...args: any[]): void;
}

class FadeCameraActionScript extends ScriptNode {
    constructor(parent: ScriptNode | Phaser.GameObjects.GameObject | Phaser.Scene);
    fadeDirection: "FadeIn" | "FadeOut";
    color: string;
    static getColor(colorName: string): Phaser.Display.Color;
    execute(...args: any[]): void;
}

class FlashCameraActionScript extends ScriptNode {
    constructor(parent: ScriptNode | Phaser.GameObjects.GameObject | Phaser.Scene);
    color: string;
    execute(...args: any[]): void;
}

class ShakeCameraActionScript extends ScriptNode {
    constructor(parent: ScriptNode | Phaser.GameObjects.GameObject | Phaser.Scene);
    intensity: number;
    force: boolean;
    execute(...args: any[]): void;
}

class ZoomCameraActionScript extends ScriptNode {
    constructor(parent: ScriptNode | Phaser.GameObjects.GameObject | Phaser.Scene);
    zoom: number;
    execute(...args: any[]): void;
}

