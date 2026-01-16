var __extends = (this && this.__extends) || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
};
var objects;
(function (objects) {
    var WeaponButton = (function (_super) {
        __extends(WeaponButton, _super);
        //protected _type;
        /**Added this weaponbutton object to currentLevel by its constructor*/
        function WeaponButton(pathString, x, y, width, height, isCentered) {
            _super.call(this, pathString, x, y, width, height, isCentered);
            this._pathString = pathString;
            this._createTowerPreview();
            this._isDragging = false;
            this.on("pressmove", this.pressmoveWeaponButton, this);
            this.on("pressup", this.pressupWeaponButton, this);
            //this._towerPreview.on("pressup", this.mouseupTowerPreview, this._towerPreview);
            currentLevel.addChild(this);
        }
        WeaponButton.prototype.pressmoveWeaponButton = function (event) {
            this._moveTowerPreview();
        };
        WeaponButton.prototype.pressupWeaponButton = function (event) {
            if (this._isDragging) {
                //console.log("pressup");
                this._createTower();
                this._resetPositionTowerPreview();
                this._isDragging = false;
            }
        };
        WeaponButton.prototype._createTower = function () {
            towers.push(new objects.Tower(assets.getResult(this._pathString), "ta", this._towerPreview.x, this._towerPreview.y, 5, 300, 20, 1));
        };
        WeaponButton.prototype._resetPositionTowerPreview = function () {
            this._towerPreview.x = -100;
            this._towerPreview.y = 0;
        };
        WeaponButton.prototype._createTowerPreview = function () {
            this._towerPreview = new createjs.Bitmap(assets.getResult(this._pathString));
            this._towerPreview.regX = 25;
            this._towerPreview.regY = 25;
            this._resetPositionTowerPreview();
            currentLevel.addChild(this._towerPreview);
        };
        WeaponButton.prototype._moveTowerPreview = function () {
            this._isDragging = true;
            this._towerPreview.x = stage.mouseX;
            this._towerPreview.y = stage.mouseY;
        };
        return WeaponButton;
    })(objects.Button);
    objects.WeaponButton = WeaponButton;
})(objects || (objects = {}));
//# sourceMappingURL=weaponbutton.js.map