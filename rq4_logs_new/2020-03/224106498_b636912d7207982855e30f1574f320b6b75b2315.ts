import { UI } from "../UI.js";
import { GameWindow, Page } from "../GameWindow.js";
import MainMenu from "./MainMenu.js";
import SettingsScreen from "./SettingsScreen.js";
import Game from "../../Game.js";
import WorldScreen from "../worldScreen/WorldScreen.js";

export default class PauseMenu implements Page {
    readonly html: HTMLElement;

    constructor(private run: Game) {
        this.html = UI.makeDiv(["main-menu"]);
        this.refresh();
    }

    refresh(): void {
        UI.fillHTML(this.html, [
            UI.makeHeader("Game Paused", 2),
            UI.makeDivContaining([
                UI.makeButton("resume game", () => {
                    GameWindow.show(new WorldScreen(this.run));
                }),
                MainMenu.makeAudioButton(this),
                UI.makeButton("change settings", () => {
                    GameWindow.show(new SettingsScreen(this));
                }),
                UI.makeButton("quit game", () => {
                    GameWindow.show(new MainMenu());
                }),
            ], ["main-menu-options"]),
        ]);
    }
}