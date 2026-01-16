"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const vscode = require("vscode");
var timerType;
(function (timerType) {
    timerType[timerType["Short"] = 1] = "Short";
    timerType[timerType["Long"] = 2] = "Long";
    timerType[timerType["Workday"] = 3] = "Workday";
    timerType[timerType["Break"] = 4] = "Break";
})(timerType || (timerType = {}));
class Timer {
    constructor(statusBarItem, statusBarTxt) {
        this.config = vscode.workspace.getConfiguration("BreakTime");
        this.redText = 0;
        this.endMessages = ["get up and do some stretching!!", "break time!"];
        this.myType = timerType.Short;
        this.intervalID = undefined;
        this.currentTime = 0; //seconds
        this.running = false;
        this.paused = false;
        this.statusBarItem = statusBarItem;
        this.statusBarTxt = statusBarTxt;
    }
    message() {
        vscode.window.showInformationMessage(`Time for a break!`, "Take a break", "Ignore").then((selectedItem) => {
            if (selectedItem == "Take a break")
                this.takeBreak();
            else
                this.start(0);
        });
    }
    setTimer() {
        //set interval that handle the count down
        if (this.myType == timerType.Break)
            this.statusBarItem.color = "green";
        this.intervalID = setInterval(() => {
            if (this.currentTime <= 0) {
                //break time!
                this.resetTimer();
                this.message();
            }
            else if (!this.paused) {
                //decrease time and update the text in UI
                this.currentTime--;
                //check settings about last minute to display or not
                if (this.currentTime <= 60 && this.config.showLastMinute === false) {
                    this.statusBarItem.text = `Break in: <1 min`;
                }
                else {
                    this.statusBarItem.text = `Break in: ${this.secToTime(this.currentTime)}`;
                }
                //last tot sec display red text
                const color = "firebrick";
                if (this.currentTime < this.redText && this.statusBarItem.color !== color) {
                    this.statusBarItem.color = color;
                }
            }
        }, 1000);
    }
    start(time) {
        if (time === 0) {
            //the user stopped the timer
            this.resetTimer();
            return;
        }
        this.currentTime = time;
        this.running = true;
        if (this.intervalID) {
            clearInterval(this.intervalID);
        }
        this.setTimer();
    }
    pauseTime() {
        this.paused = true;
        this.running = false;
        this.statusBarItem.text = "Paused";
    }
    continuePaused() {
        this.paused = false;
        this.running = true;
        this.start(this.currentTime);
    }
    isPaused() {
        return this.paused;
    }
    isRunning() {
        return this.running;
    }
    propagateBreak() {
    }
    takeBreak() {
        this.statusBarItem.color = "green";
        this.start(60);
    }
    resetTimer() {
        this.statusBarItem.text = this.statusBarTxt;
        clearInterval(this.intervalID);
        this.statusBarItem.color = undefined;
        this.currentTime = 0;
        this.intervalID = undefined;
        this.running = false;
        this.statusBarItem.color = undefined;
    }
    secToTime(totalSecs) {
        const hours = Math.floor(totalSecs / 3600);
        const minutes = Math.floor((totalSecs % 3600) / 60);
        const seconds = Math.floor(totalSecs % 60);
        return `${hours > 0 ? `${hours}h ` : ""}${minutes > 0 ? `${minutes}min ` : ""}${totalSecs <= 60 ? `${seconds}sec` : ""}`;
    }
}
exports.Timer = Timer;
//# sourceMappingURL=Timer.js.map