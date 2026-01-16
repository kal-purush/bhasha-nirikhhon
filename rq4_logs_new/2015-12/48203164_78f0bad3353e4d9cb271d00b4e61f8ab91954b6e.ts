interface DialogAction {
    (): void;
}

interface Dialog {
    text: string;
    name: string;
    sprite: string;
    action?: DialogAction;
}

class ChatBoxBehavior extends Sup.Behavior {
    sprite: Sup.SpriteRenderer;
    dialogText: Sup.TextRenderer;
    pendingDialogs: Dialog[];
    nextAction: DialogAction;

    awake() {
        this.actor.setVisible(false);
        this.sprite = this.actor.getChild("Face").spriteRenderer;
        this.dialogText = this.actor.getChild("Dialog").textRenderer;
    }

    update() {
        if (this.actor.getVisible()) {
            if (Sup.Input.wasKeyJustPressed("X")) {
                if (this.nextAction != null) {
                    this.nextAction();
                    this.nextAction = null;
                }
                if (this.pendingDialogs != null && this.pendingDialogs.length > 0) {
                    this.showDialog_internal(this.pendingDialogs);
                }
                else {
                    this.actor.setVisible(false);
                }
            }
        }
    }
    
    public showDialog_internal(dialog: Dialog[]) {
        this.actor.setVisible(true);
        let currentDialog = dialog[0];
        this.sprite.setSprite(currentDialog.sprite);
        this.dialogText.setText(this.formatDialog(currentDialog.name + ": " + currentDialog.text));
        this.nextAction = currentDialog.action;
        dialog.shift();
        this.pendingDialogs = dialog;
    }
    
    public showDialog(dialog: Dialog[]) {
        this.actor.setVisible(true);
        this.pendingDialogs = dialog;
    }

    formatDialog(dialog: string) {
        let formatedDialog = "";
        let slicedString = dialog.split(" ");
        let charCount = 0;
        let maxCharPerLine = 18;
        for (let i = 0; i < slicedString.length; i += 1) {
            let sliceOfDialog = slicedString[i];
            if (charCount + sliceOfDialog.length + 1 > maxCharPerLine) {
                formatedDialog += "\n";
                charCount = 0;
            }
            charCount += sliceOfDialog.length + 1;
            formatedDialog += sliceOfDialog + " ";
        }
        return formatedDialog;
    }
}
Sup.registerBehavior(ChatBoxBehavior);