import { Page, GameWindow } from "../GameWindow.js";
import Game from "../../Game.js";
import { UI } from "../UI.js";
import Ending from "../../quests/Ending.js";
import EndScreen from "../endScreen/EndScreen.js";

export default class QuestIndicator implements Page {

    readonly html: HTMLElement;
    private run: Game;

    constructor(run: Game) {
        this.html = UI.makeDiv(["world-screen-quest-description"]);
        this.run = run;
        this.refresh();
    }

    refresh(): void {
        const questDescription = this.run.getCurrentQuestDescription();
        const questHint = this.run.getCurrentQuestHint();
        const questText = questHint ? `${questDescription}\n(hint: ${questHint})` : questDescription;
        let questLabel = UI.makePara(`Objective: ${questText}`);

        const prevQuestDescription = this.run.getPreviousQuestDescription();
        if (!this.run.questCompletionShown) {
            this.run.questCompletionShown = true;
            questLabel = UI.makePara(`Completed: ${prevQuestDescription}`, ["quest-description-emphasis"]);

            // reset description after time has passed
            setTimeout(() => {
                const endState: Ending | undefined = this.run.getQuestEndState();
                if (endState) {
                    GameWindow.show(new EndScreen(endState));
                } else {
                    this.refresh();
                }
            }, 1500);
        }

        UI.fillHTML(this.html, [questLabel]);
    }
}