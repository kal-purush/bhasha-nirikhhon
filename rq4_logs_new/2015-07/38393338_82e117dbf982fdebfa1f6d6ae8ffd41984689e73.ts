class BeingFactory {
    builders: any[];
    cellFactory: any = new CellFactory();

    constructor() {
        this.builders = [];
        this.generateBuilders();
    }

    createBeing(beingType, data) {
        return this.builders[beingType](data);
    }

    generateBuilders() {
        var self = this;
        this.builders[BeingTypes.PLAYER] = function (data: any) { 
            var core = self.cellFactory.createCell(CellTypes.CORE, new coreData());
            var being = new PlayerBeing(core, data);
            return being;
        }

        this.builders[BeingTypes.NPC] = function (data: any) {
            var core = self.cellFactory.createCell(CellTypes.CORE, new coreData());
            core.image.graphics.beginFill("green").drawCircle(cellSize / 2, cellSize / 2, cellSize / 4);
            var being = new PlayerBeing(core, data);
            return being;
        }
    }
}