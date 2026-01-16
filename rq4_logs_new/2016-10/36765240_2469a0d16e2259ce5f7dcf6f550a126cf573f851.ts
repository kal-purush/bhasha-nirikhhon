// Description:
//  Beat the sniperino
//  
//
// Commands:
//  sniperino( me)? - start a new game of sniperino 
//  snipe( me)? - roll a snipe roll in an attempt to beat the sniperino
//
// Author:
//  Steve Shipsey

import { Robot } from "tsbot";

interface ISniper {
    name: string;
    gamesPlayed: number;
    gamesWon: number;
    currentSnipe?: number;

    getWinRate(): number;
    getNewSnipe(): number;
}

class Sniper implements ISniper {
    public name: string;
    public gamesPlayed: number;
    public gamesWon: number;
    public currentSnipe: number;
    
    constructor(n: string) {
        this.name = n.slice(0, n.indexOf(" "));
        this.gamesPlayed = 0;
        this.gamesWon = 0;
        this.currentSnipe = this.getNewSnipe();
    }

    public getWinRate(): number {
        return this.gamesPlayed > 0 ? (this.gamesWon / this.gamesPlayed) * 100 : 0;
    }

    public getNewSnipe(): number {
        return Math.floor(Math.random() * 99) + 1;
    }
}

let game = (robot: Robot) => {
    robot.respond(/sniperino( me)?/i, (res) => {
        let name = res.message.user.name;
        if (robot.brain.get(name) == null) {
            robot.brain.set(name, new Sniper(name));
        }
        let sniper = robot.brain.get(name);
        res.send("ヽ༼ຈل͜ຈ༽_•︻̷̿┻̿═━一 ༼ つ ಥ_ಥ ༽つ" + sniper.name + ", roll higher than a " + sniper.currentSnipe + " or the donger gets it!");    
    });
};

export = game;
