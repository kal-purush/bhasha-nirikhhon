enum ERROR_LEVEL { DEBUG = 0, INFO = 1, WARN = 2, ERROR = 3 };
var globalErrLevel: ERROR_LEVEL = ERROR_LEVEL.ERROR;
var myDebug = function(str: String, level: ERROR_LEVEL = ERROR_LEVEL.INFO): void {
    if (level > globalErrLevel) console.log(str);
}

class Queue<TElement>{
    queueData: QueueData<TElement>;
    constructor(queueData_: QueueData<TElement>) {
        this.queueData = queueData_;
    }
    push(element: TElement): void {
        this.queueData.pushArray.push(element);
    }
    private transfer(): void {
        if (this.queueData.popArray.length == 0) {
            this.queueData.popArray = this.queueData.pushArray;
            this.queueData.popArray.reverse();
            this.queueData.pushArray = [];
        }
    }
    pop(): TElement {
        this.transfer();
        if (this.queueData.popArray.length == 0)
            return null;
        else
            return this.queueData.popArray.pop();
    }
    top(): TElement {
        if (this.queueData.popArray.length == 0)
            return null;
        else
            return this.queueData.popArray[this.queueData.popArray.length - 1];
    }
}



module.exports.loop = function() {
    myDebug("Main");
    for (var spawnId in Game.spawns) {
        var spawn: Spawn = Game.spawns[spawnId];
        spawn.createCreep([WORK, CARRY, MOVE, MOVE]);
    }
};