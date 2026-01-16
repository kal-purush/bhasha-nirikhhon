/// <reference path="..\lib\easeljs.d.ts" />

class GraphicsEngine {
    stage: createjs.Stage;
    menu: Menu;
    fpsPoint: Point;
    currentZoom = 1;
    rulerOn = false;
    player: PlayerBeing;
    gameCanvas: any;
    cellMenuCanvas: any;
    cellMenuCanvasWidth: number = 200;

    
    constructor(gameCanvas, cellMenuCanvas, player) {
        this.setGameCanvasProps(gameCanvas);
        this.setCellMenuCanvasProps(cellMenuCanvas);
        this.setPlayer(player, this.gameCanvas);
        this.stage = new createjs.Stage(this.gameCanvas);
        this.menu = new Menu();
        this.menu.createRuler(this.gameCanvas);
        this.fpsPoint = new Point(this.gameCanvas.width - 60, this.gameCanvas.height - 20);
    }

    setGameCanvasProps(gameCanvas) {
        this.gameCanvas = gameCanvas;
        this.gameCanvas.style = "border:1px solid blue;"
        this.gameCanvas.width = $(window).width() - this.cellMenuCanvasWidth;
        this.gameCanvas.height = $(window).height();
    }

    setCellMenuCanvasProps(cellMenuCanvas) {
        this.cellMenuCanvas = cellMenuCanvas;
        this.cellMenuCanvas.style = "border:1px solid blue;"
        this.cellMenuCanvas.width = this.cellMenuCanvasWidth;
        this.cellMenuCanvas.height = $(window).height();
    }

    render(objects, fps) {
        this.updateCanvasPosition(objects);
        this.syncStage(objects);
        this.menu.update(fps, this.fpsPoint);
        this.stage.update();
    }

    setPlayer(player, canvas) {
        this.player = player;
    }

    updateCanvasPosition(objects) {
        var self = this;
        objects.forEach(function (object) {
            object.image.x = object.gameRect.center.x;
            object.image.y = object.gameRect.center.y;
        });
    }

    syncStage(objects) {
        var stage = this.stage;
        stage.removeAllChildren();
        objects.forEach(function (obj) {
            stage.addChild(obj.image);
        });
        stage.addChild(this.menu.fpsText);
        if (this.rulerOn) {
            stage.addChild(this.menu.ruler)
        }
    }

} 