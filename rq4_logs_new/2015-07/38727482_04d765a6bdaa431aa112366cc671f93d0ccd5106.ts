module objects {
    export class Button extends objects.GameObject {

        // PRIVATE INSTANCE VARIABLES +++++++++++++++++++++++++++++++++++++++++++++++++

        constructor(stringPath: string, x: number, y: number) {
            super(stringPath);

            this.x = x;
            this.y = y;
            this.addEventListener("mouseover", this._buttonOver);
            this.addEventListener("mouseout", this._buttonOut);
        }

        // EVENT HANDLERS

        private _buttonOut(event: createjs.MouseEvent): void {
            event.currentTarget.alpha = 1.0;
        }

        private _buttonOver(event: createjs.MouseEvent): void {
            event.currentTarget.alpha = 0.5;
        }

    }

} 