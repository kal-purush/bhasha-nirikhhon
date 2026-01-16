class CellMenu {
    cellNameTextSize = 14;
    cellNameTextColor = "red";
    stage: createjs.Stage;
    cellFactory: CellFactory;
    startPoint = new Point(10, 0);
    lineHeight = 30;
    cellTargetSize = 20;
    highlightedLineContainer: createjs.Container;

    constructor(canvas, cellFactory: CellFactory) {
        this.stage = new createjs.Stage(canvas);
        this.cellFactory = cellFactory;
    }

    update() {

    }

    highlightLineContainer() {

    }

    createCellsInfo(cells: number[]) {
        var textX = this.startPoint.x + 30;
        var y = this.startPoint.y;

        var scaleFactor = this.cellTargetSize / cellSize;

        for (var i = 0; i < cells.length; i++) {
            var lineContainer = new createjs.Container();
            var fillShape = new createjs.Shape();
            fillShape.graphics.beginFill("white").drawRect(0, 0, 300, this.lineHeight);
            var cellType = cells[i];
            var cell = this.cellFactory.createCell(cellType, new Object());
            cell.image.scaleX = scaleFactor;
            cell.image.scaleY = scaleFactor;
            var text = new createjs.Text(cell.gameType, this.cellNameTextSize + "px Arial", this.cellNameTextColor);
            text.x = textX;

            cell.image.y = this.lineHeight / 6;
            text.y = this.lineHeight / 6;
            lineContainer.y = y;
            y += this.lineHeight;
            fillShape.name = cell.gameType;
            cell.image.name = cell.gameType;
            text.name = cell.gameType;
            lineContainer.addChild(fillShape, cell.image, text);
            this.stage.addChild(lineContainer);
        }
    }
}