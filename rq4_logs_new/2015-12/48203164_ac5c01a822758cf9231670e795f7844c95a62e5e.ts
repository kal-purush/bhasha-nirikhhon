class MenuGiveQuestBehavior extends Sup.Behavior {
    items = [
        "String",
        "Rope",
        "Leather",
        "Spruce",
        "Copper",
        "Iron",
        "Fur",
        "Ebony",
        "Gold",
        "Silver",
        "Silk",
        "Bamboo",
        "Diamond",
    ];
    selection = 0;
    menuOffset = 0;
    entriesVisibleLenght = 8;
    entriesSpacing = 0.25;
    entriesActor: Sup.Actor;
    cursorActor: Sup.Actor;
    
    awake() {
        this.entriesActor = new Sup.Actor("entries", this.actor);
        this.entriesActor.setLocalPosition(new Sup.Math.Vector3(0, 0, 0));
        for (var i = 0; i < 8 && i < this.items.length; ++i) {
            let anEntry = new Sup.Actor("entry"+i, this.entriesActor);
            new Sup.TextRenderer(anEntry, this.items[i], "Medieval font", { "alignment": "left" });
            anEntry.setLocalPosition(new Sup.Math.Vector3(0.2, -this.entriesSpacing * i, 0));
        }
        this.cursorActor = new Sup.Actor("cursor", this.actor);
        new Sup.TextRenderer(this.cursorActor, "*", "Medieval font");
        this.actor.setVisible(false);
    }

    update() {
        if (this.actor.getVisible()) {
            if (Sup.Input.wasKeyJustPressed("C")) {
                this.setFocus(false);
                Sup.getActor("Player").getBehavior(PlayerBehavior).giveControlBack();
                Sup.Audio.playSound("Back");
            }
            if (Sup.Input.wasKeyJustPressed("UP")) {
                if (this.selection > 0) {
                    this.selection -= 1;
                    Sup.Audio.playSound("Blip");
                }
            }
            if (Sup.Input.wasKeyJustPressed("DOWN")) {
                if (this.selection < this.items.length - 1) {
                    this.selection += 1;
                    Sup.Audio.playSound("Blip");
                }
            }
            if (this.selection < this.menuOffset) {
                this.menuOffset = this.selection;
                this.updateMenuEntries();
            }
            if (this.selection > this.menuOffset + (this.entriesVisibleLenght - 1)) {
                this.menuOffset = this.selection - (this.entriesVisibleLenght - 1);
                this.updateMenuEntries();
            }
            this.cursorActor.setLocalY(-this.entriesSpacing * (this.selection - this.menuOffset));
        }
    }

    updateMenuEntries() {
        let children = this.entriesActor.getChildren();
        Sup.log(children.length);
        for (var i = 0; i < children.length; ++i) {
            let anEntry = children[i];
            anEntry.textRenderer.setText(this.items[i + this.menuOffset]);
        }
    }

    public setFocus(focus: boolean) {
        if (focus) {
            this.selection = 0;
            this.actor.setVisible(true);
        }
        else {
            this.actor.setVisible(false);
        }
    }
}
Sup.registerBehavior(MenuGiveQuestBehavior);