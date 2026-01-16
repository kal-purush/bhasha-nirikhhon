module FreeHand {

    export interface Command {
        redo();
        undo();
    }

    export class CommandList {
        private commands: Command[] = [];
        private currentIndex: number = 0;

        constructor() {}

        add(command: Command) {
            this.commands.length = this.currentIndex; // clip to the current undo level
            this.commands.push(command);
            this.currentIndex = this.commands.length; // past the end of the list
            command.redo();
        }

        reset() {
            this.commands.length = 0;
            this.currentIndex = 0;
        }

        undo() {
            if (this.currentIndex <= 0)
                return; // nothing left to undo

            this.currentIndex--;
            this.commands[this.currentIndex].undo();
        }

        redo() {
            if (this.currentIndex >= this.commands.length)
                return; // nothing undone

            this.commands[this.currentIndex].redo();
            this.currentIndex++;
        }

        canUndo(): boolean {
            return this.currentIndex > 0 && this.commands.length > 0;
        }

        canRedo(): boolean {
            return this.currentIndex < this.commands.length;
        }
    }

}