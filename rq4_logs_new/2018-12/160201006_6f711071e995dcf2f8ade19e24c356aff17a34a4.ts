import * as bluebird from 'bluebird';

export interface Creature {
    hp: number;
    type: string;
    id: number;
    round: number;
    location: { x: number; y: number; }
}

export class DnD {
    public static fs = bluebird.Promise.promisifyAll(require('fs'));
    public static Creatures: Creature[] = [];

    public static run = async (): Promise<number> => {
        let mine = await DnD.getMine();
        let stopFight = false;
        let round = 0;
        while (!stopFight) {
            round++;
            stopFight = true;
            DnD.Creatures.sort(DnD.sort);
            for (let c of DnD.Creatures) {
                let enemies = DnD.FindEnemies(c);
                if (!enemies) {
                    stopFight = true;
                    break;
                }
                c.round = round;
            }

            DnD.Creatures = DnD.Creatures.filter((c) => {
                if (c.hp > 0) {
                    return true;
                }
                mine[c.location.y][c.location.x] = ".";
                return false;
            });
        }
        return 0;
    }

    public static FindNextStep = (npc: Creature, enemies: number[]) => {
        let e = DnD.Creatures.filter((x) => enemies.indexOf(x.id) > -1);

        for (let c of e) {

        }
    }

    public static FindEnemies = (npc: Creature): number[] => {
        return [];
    }

    public static sort = (a: Creature | { x: number, y: number }, b: Creature | { x: number, y: number }): number => {
        let aLoc = (a as any).location || a;
        let bLoc = (b as any).location || b;
        let y = aLoc.y - bLoc.y;
        if (y !== 0) {
            return y;
        }

        return aLoc.x - bLoc.x;
    }

    public static getMine = async (): Promise<string[][]> => {
        let raw: string = await DnD.fs.readFileAsync("input/dayOne.txt", { encoding: "utf8" });
        let cleaned = raw.split("\n");
        let g = 0;
        let e = 0;
        let map: string[][] = cleaned.map((row, y) => {
            let yv = row.split('');
            return yv.map((xv, x) => {
                if (xv === "E") {
                    let elf: Creature = {
                        hp: 200,
                        type: xv,
                        id: e,
                        round: 0,
                        location: { x: x, y }
                    };
                    DnD.Creatures.push(elf);
                    e++;
                }
                if (xv === "G") {
                    let goblin: Creature = {
                        hp: 200,
                        type: xv,
                        id: g,
                        round: 0,
                        location: { x: x, y }
                    };
                    DnD.Creatures.push(goblin);
                    g++;
                }
                return xv;
            });
        });

        return map;
    }

}