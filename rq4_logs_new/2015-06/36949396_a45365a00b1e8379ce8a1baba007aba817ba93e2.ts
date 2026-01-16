/**
 * Manages the state of the various achievements in the game.
 */
class AchievementTracker {
    private game: Game;
    private totalUnlocked = new Component(0);
    private totalAchievements = new Component(0);

    constructor(game: Game) {
        this.game = game;

        this.createAchievements();
    }

    private createAchievements(): void {
        const game = this.game;
        this.create("Gold Digger", "gold_coin.png", game["gold"], 1000, "Earn {$} gold.");
        this.create("Alchemist's Bane", "transmute.png", game["gold"], 1e6, "Earn {$} gold.");
        this.create("Longevity", "longevity.png", game["totalTimePlayed"], 5*3600*1000, "Play for {$}.", $.timeSpan);
    }

    private create(name: string, icon: string, cmp: Component, value: number, description: string, formatter?: ValueFormatter): void {
        // The state of the achievement
        let isUnlocked = false;

        if (!formatter) {
            formatter = $.commify;
        }

        // Create the element representing the achievement
        const element = document.createElement("div");
        element.className = "achievement";
        element.innerHTML = "<span class='achievement-mask'></span>";
        element.style.backgroundImage = `url('resources/${icon}')`;
        $.id("achievement-container").appendChild(element);

        // Show description in tooltip
        description = description.replace("{$}", formatter(value));
        Tooltip.attachFunc(element, () => {
            // Text that appears in the tooltip of the achievement.
            // Kinda messy, but works fine...
            let progress;
            if (isUnlocked) {
                progress = `<div style="margin-top: 8px;color: #22cc22;">Unlocked</div>`;
            } else {
                let pp = formatter(cmp.val) + " / " + formatter(value);
                progress = `<div style="margin-top: 8px;color: #999999; font-size: 11px;">Progress: ${pp}</div>`;
            }
            return `<div style="margin-bottom: 6px; font-size: 16px; color: #ff7700;">${name}</div>`
                + `<div style="font-size: 12px; color: #cccccc;">${description}</div>`
                + `${progress}`;
        });

        // Add listener to component
        cmp.whenReached(value, () => {
            // Add the unlocked achievement class
            element.className += " achievement-unlocked";
            // Clear the 'locked achievement' mask
            element.innerHTML = "";
            // Mark as unlocked
            isUnlocked = true;

            console.log("Achievement unlocked:", name);
            // Increment unlocked achievement count
            this.totalUnlocked.val++;
        });

        // Increment total achievement count
        this.totalAchievements.val++;
    }
}