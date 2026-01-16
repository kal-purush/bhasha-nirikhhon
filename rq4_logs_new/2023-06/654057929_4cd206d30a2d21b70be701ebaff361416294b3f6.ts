import { KeyCodes } from "@hazel/share";
import { Input } from "@pw/Hazel/Hazel/Input";
import { OrthographicCamera } from "./OrthographicCamera";
import {
    HazelEvent,
    EventDispatcher,
    MouseScrolledEvent,
    WindowResizeEvent,
} from "@pw/Hazel/Hazel/Events";
import { vec3 } from "gl-matrix";
import { GuiLayer } from "../Gui";

export class OrthographicCameraController {
    constructor(aspectRatio: number, rotation: boolean = false) {
        this.#rotation = rotation;
        this.#aspectRatio = aspectRatio;
        this.#orthographicCamera = new OrthographicCamera(
            this.#aspectRatio * -this.#zoomLevel,
            this.#aspectRatio * this.#zoomLevel,
            -this.#zoomLevel,
            this.#zoomLevel,
        );
        this.initDebugGui();
    }

    //#region Public Methods
    onUpdate(ts: number): void {
        //#region Camera Control
        if (Input.isKeyPressed(KeyCodes.KeyA)) {
            this.#cameraPosition[0] += this.#cameraTransitionSpeed * ts;
        } else if (Input.isKeyPressed(KeyCodes.KeyD)) {
            this.#cameraPosition[0] -= this.#cameraTransitionSpeed * ts;
        }
        if (Input.isKeyPressed(KeyCodes.KeyW)) {
            this.#cameraPosition[1] -= this.#cameraTransitionSpeed * ts;
        } else if (Input.isKeyPressed(KeyCodes.KeyS)) {
            this.#cameraPosition[1] += this.#cameraTransitionSpeed * ts;
        }
        this.#orthographicCamera.setPosition(this.#cameraPosition);

        if (this.#rotation) {
            if (Input.isKeyPressed(KeyCodes.KeyQ)) {
                this.#cameraRotation -= this.#cameraRotationSpeed * ts;
            } else if (Input.isKeyPressed(KeyCodes.KeyE)) {
                this.#cameraRotation += this.#cameraRotationSpeed * ts;
            }
            this.#orthographicCamera.setRotation(this.#cameraRotation);
        }
        //#endregion
    }

    onEvent(event: HazelEvent): void {
        const dispatcher = new EventDispatcher(event);
        dispatcher.dispatch(
            MouseScrolledEvent,
            this.onMouseScrolled.bind(this),
        );
        dispatcher.dispatch(WindowResizeEvent, this.onWindowResized.bind(this));
    }

    getCamera(): OrthographicCamera {
        return this.#orthographicCamera;
    }
    //#endregion

    //#region Private Methods

    private onMouseScrolled(event: MouseScrolledEvent): boolean {
        this.#zoomLevel -= event.getYOffset() * this.#zoomLevelSpeed;
        this.#zoomLevel = Math.max(this.#zoomLevel, this.#zoomLevelMin);
        this.calcCameraProjection();
        return false;
    }

    private onWindowResized(event: WindowResizeEvent): boolean {
        this.#aspectRatio = event.getWidth() / event.getHeight();
        this.calcCameraProjection();
        return false;
    }

    private calcCameraProjection() {
        this.#orthographicCamera.setProjection(
            this.#aspectRatio * -this.#zoomLevel,
            this.#aspectRatio * this.#zoomLevel,
            -this.#zoomLevel,
            this.#zoomLevel,
        );
    }

    private initDebugGui() {
        GuiLayer.begin("Camera Controller");
        const proxy = GuiLayer.proxy(
            "zoomLevel",
            () => this.#zoomLevel,
            (l) => {
                this.#zoomLevel = l;
                this.calcCameraProjection();
            },
        );
        GuiLayer.add(proxy, "zoomLevel")
            .min(this.#zoomLevelMin)
            .max(10)
            .step(this.#zoomLevelSpeed);
        GuiLayer.end();
    }

    //#endregion

    //#region Private Fields
    #orthographicCamera: OrthographicCamera;
    #aspectRatio: number;
    #zoomLevel = 1.0;
    #zoomLevelSpeed = 0.0025;
    #zoomLevelMin = 0.25;

    #cameraPosition = vec3.create();
    #cameraTransitionSpeed = 1;

    #rotation: boolean;
    #cameraRotation = 0;
    #cameraRotationSpeed = 180;
    //#endregion
}