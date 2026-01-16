/// <reference path="Field.ts" />
/// <reference path="TurnManager.ts" />

namespace MainApp {
    /**
     * GameMaster
     */
    export class GameMaster {
        private field: Field;
        private turnManager: TurnManager;
        constructor() {
            let size: Size = { width: 3, height: 3 };
            this.field = new Field(size);

            var player1 = new Player();
            player1.pieces = new Array(5).map(() => new Circle());
            var player2 = new Player();
            player2.pieces = new Array(4).map(() => new Cross());
            this.turnManager = new TurnManager();
            this.turnManager.players = [player1, player2];
        }
    }
}