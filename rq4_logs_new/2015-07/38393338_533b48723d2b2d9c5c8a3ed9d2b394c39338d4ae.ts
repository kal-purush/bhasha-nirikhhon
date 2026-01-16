class ControlsManager {
    canvas: any;
    inputsContainer: InputsContainer;

    constructor() {
        this.canvas = document.getElementById("gameCanvas");
        this.inputsContainer = new InputsContainer();
        this.setInputHandlers();
    }

    setInputHandlers() {
        var self = this;
        document.oncontextmenu = function () { return false };

        $(self.canvas).mousedown(function (e) {
            if (e.button === 2) {
                self.inputsContainer.latestRightMouseClick = new Point(e.pageX - self.canvas.offsetLeft, e.pageY - self.canvas.offsetTop);
                //console.log("Mouse pos: " + self.latestRightMouseClick.x + " " + self.latestRightMouseClick.y);
            }
            return false;
        }); 

        window.onkeydown = function (e) {
            console.log(e.keyCode);
            return false;
        }
        window.onkeyup = function (e) {
            return false;
        }
    }

    reportInputs() {
        return this.inputsContainer.latestRightMouseClick;
    }

    kkk() {}
}


class InputsContainer {
    latestRightMouseClick: Point;

}