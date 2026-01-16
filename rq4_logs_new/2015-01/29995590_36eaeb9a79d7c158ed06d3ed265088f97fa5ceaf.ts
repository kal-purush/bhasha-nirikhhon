import V = require('../Utils/Variables');
import W = require('./World');
import T = require('./Tiles');
import P = require('./Player');

export interface ICommand {
    Run();
    CanRun(): boolean;
}

export function getCommand(data, idPlayer: string): ICommand {
    var commandName = data.command;
    switch (commandName) {
        case "LoginCommand":
            return new LoginCommand(data, idPlayer);
        case "AddHumidityCommand":
            return new AddHumidityCommand(data, idPlayer);
        case "AddElevationCommand":
            return new AddElevationCommand(data, idPlayer);
        case "AddWhirlwindCommand":
            return new AddWhirlwindCommand(data, idPlayer);
        case "RemoveHumidityCommand":
            return new RemoveHumidityCommand(data, idPlayer);
        case "RemoveElevationCommand":
            return new RemoveElevationCommand(data, idPlayer);
        case "RemoveWhirlwindCommand":
            return new RemoveWhirlwindCommand(data, idPlayer);
    }
    return null;
}

export class LoginCommand implements ICommand {

    login: string;

    constructor(data, public idPlayer: string) {
        this.login = data.login;
    }

    Run() {
        var world = W.World.Instance;

        var player = new P.Player(this.idPlayer, this.login);

        world.addPlayer(this.idPlayer, player);
        var color = -1;
        var end = false;
        for (var i = 0; i < world.temples.length && !end; i++) {
            var temple = world.temples[i];

            if (temple.player == null && color == -1) {
                color = temple.color;
                temple.player = player;
                player.color = color;
            } else if (temple.color == color) {
                temple.player = player;
            }
        }
    }

    CanRun() {
        return Object.keys(W.World.Instance.players).length < V.Variables.nbPlayerMax;
    }
}

export class BasePowerCommand implements ICommand {

    x: number;
    y: number;
    player: P.Player;
    brushSize: number;
    powerCost: number;

    constructor(data, idPlayer: string) {
        this.x = data.x;
        this.y = data.y;
        this.player = W.World.Instance.getPlayer(idPlayer);
    }

    removeFaith(quantity: number) {
        if (this.player != null) {
            //this.player.faithScore -= quantity;
        }
        //// Easter Egg
        //if (this.player.faithScore == 42) {
        //    this.player.faithScore = 4200
        //    // Todo : play song
        //}
    }

    Run() {
        this.calculateNewCodes();
        this.removeFaith(this.powerCost);
    }

    CanRun() {
        return true; //this.player.faithScore >= this.powerCost;
    }

    getImpactedTiles() {
        var result = new Array<T.ITile>();

        var width = V.Variables.worldWidth;
        var height = V.Variables.worldHeight;

        for (var i = this.x - this.brushSize; i <= this.x + this.brushSize; i++) {
            for (var j = this.y - this.brushSize; j <= this.y + this.brushSize; j++) {
                var x = ((i % width) + width) % width;
                var y = ((j % height) + height) % height;

                result.push(W.World.Instance.getTile(x, y));
            }
        }
        return result;
    }

    calculateNewCodes() {
        this.getImpactedTiles().forEach(function (tile) {
            tile.calculateNewCode();
        });
    }

    processMountain(tile: T.ITile, brushSize: number, index: number) {
        if (tile.elevation == 1) {

            var x = index % (brushSize * 2 + 1);
            var y = Math.floor(index / (brushSize * 2 + 1));
            var typ = T.MountainType.Full;
            if (x == 0 && y == 0)//&& (tile.mountainType != T.MountainType.TopLeft || (tile.mountainType != T.MountainType.None && tile.mountainType != T.MountainType.Full)))
                typ = T.MountainType.TopLeft;
            else if (x == 0 && y == brushSize * 2)// && (tile.mountainType != T.MountainType.TopRight || (tile.mountainType != T.MountainType.None && tile.mountainType != T.MountainType.Full)))
                typ = T.MountainType.TopRight;
            else if (x == brushSize * 2 && y == 0)//&& (tile.mountainType != T.MountainType.BottomLeft || (tile.mountainType != T.MountainType.None && tile.mountainType != T.MountainType.Full)))
                typ = T.MountainType.BottomLeft;
            else if (x == brushSize * 2 && y == brushSize * 2)//&& (tile.mountainType != T.MountainType.BottomRight || (tile.mountainType != T.MountainType.None && tile.mountainType != T.MountainType.Full)))
                typ = T.MountainType.BottomRight;

            tile.mountainType = typ;
        }
        else {
            tile.mountainType = T.MountainType.None;
        }
    }
}

export class AddHumidityCommand extends BasePowerCommand implements ICommand {

    constructor(data, idPlayer: string) {
        super(data, idPlayer);
        this.brushSize = V.Variables.humidityBrushSize;
        this.powerCost = V.Variables.humidityCost;
    }

    Run() {
        this.getImpactedTiles().forEach(function (tile) {
            if (tile.humidity != 1 && tile.indestructibleType == T.IndestructibleType.None)
                tile.humidity++;
        });

        this.player.humidityLastUseTime = 0;

        super.Run();
    }

    CanRun() {
        return super.CanRun() && this.player.canUseHumidity();
    }
}

export class AddElevationCommand extends BasePowerCommand implements ICommand {

    constructor(data, idPlayer: string) {
        super(data, idPlayer);
        this.brushSize = V.Variables.elevationBrushSize;
        this.powerCost = V.Variables.elevationCost;
    }

    Run() {

        var tiles = this.getImpactedTiles();

        for (var i = 0; i < tiles.length; i++) {

            var tile = tiles[i];

            if (tile.elevation != 1 && tile.indestructibleType == T.IndestructibleType.None)
                tile.elevation++;

            this.processMountain(tile, this.brushSize, i);
        }

        this.player.elevationLastUseTime = 0;

        super.Run();
    }

    CanRun() {
        return super.CanRun() && this.player.canUseElevation();
    }
}

export class AddWhirlwindCommand extends BasePowerCommand implements ICommand {

    constructor(data, idPlayer: string) {
        super(data, idPlayer);
        this.brushSize = V.Variables.whirlwindBrushSize;
        this.powerCost = V.Variables.whirlwindCost;
    }

    Run() {
        this.getImpactedTiles().forEach(function (tile) {
            if (tile.whirlwind != 1 && tile.indestructibleType == T.IndestructibleType.None)
                tile.whirlwind++;
        });

        this.player.whirlwindLastUseTime = 0;

        super.Run();
    }

    CanRun() {
        return super.CanRun() && this.player.canUseWhirlwind();
    }
}

export class RemoveHumidityCommand extends BasePowerCommand implements ICommand {

    constructor(data, idPlayer: string) {
        super(data, idPlayer);
        this.brushSize = V.Variables.humidityBrushSize;
        this.powerCost = V.Variables.humidityCost;
    }

    Run() {
        this.getImpactedTiles().forEach(function (tile) {
            if (tile.humidity != -1 && tile.indestructibleType == T.IndestructibleType.None)
                tile.humidity--;
        });

        this.player.humidityLastUseTime = 0;

        super.Run();
    }

    CanRun() {
        return super.CanRun() && this.player.canUseHumidity();
    }
}

export class RemoveElevationCommand extends BasePowerCommand implements ICommand {

    constructor(data, idPlayer: string) {
        super(data, idPlayer);
        this.brushSize = V.Variables.elevationBrushSize;
        this.powerCost = V.Variables.elevationCost;
    }

    Run() {
        this.getImpactedTiles().forEach(function (tile) {
            if (tile.elevation != -1 && tile.indestructibleType == T.IndestructibleType.None)
                tile.elevation--;
            tile.mountainType = T.MountainType.None;
        });

        this.player.elevationLastUseTime = 0;

        super.Run();
    }

    CanRun() {
        return super.CanRun() && this.player.canUseElevation();
    }
}

export class RemoveWhirlwindCommand extends BasePowerCommand implements ICommand {

    constructor(data, idPlayer: string) {
        super(data, idPlayer);
        this.brushSize = V.Variables.whirlwindBrushSize;
        this.powerCost = V.Variables.whirlwindCost;
    }

    Run() {
        this.getImpactedTiles().forEach(function (tile) {
            if (tile.whirlwind != -1 && tile.indestructibleType == T.IndestructibleType.None)
                tile.whirlwind--;
        });

        this.player.whirlwindLastUseTime = 0;

        super.Run();
    }

    CanRun() {
        return super.CanRun() && this.player.canUseWhirlwind();
    }
}