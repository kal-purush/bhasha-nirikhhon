import * as bluebird from 'bluebird';
import { max } from 'moment';

export interface Bot {
    x: number;
    y: number;
    z: number;
    r: number;
}
export class Nanos {
    public static fs = bluebird.Promise.promisifyAll(require('fs'));
    public static run = async (): Promise<string> => {
        let input = await Nanos.getProgram();

        let best = Nanos.getMaxR(input);

        let inR = input.filter((x) => Nanos.getInR(best, x));
        return inR.length.toString();
    }

    public static key = (bot: Bot): string => {
        return bot.x + "," + bot.y + "," + bot.z;
    }

    public static runT = async (): Promise<string> => {
        let input = await Nanos.getProgram();
        let maxSpot: { c: number, x: number, y: number, z: number } = { c: -1, x: -1, y: -1, z: -1 };
        let maxes = Nanos.getMaxes(input);
        let mins = Nanos.getMins(input);
        console.log("Max:" + maxes.x + "," + maxes.y + "," + maxes.z);
        console.log("Min:" + mins.x + "," + mins.y + "," + mins.z);
        let home: Bot = { x: 0, y: 0, z: 0, r: 0 };


        let bob: { [id: string]: { b: Bot, has: Bot[], within: Bot[] } } = {};
        input.forEach((b) => {
            let key = Nanos.key(b);
            bob[key] = {
                b, has: [], within: []
            } as any;

            input.forEach((bb) => {
                let w = Nanos.getInR(bb, b);
                let h = Nanos.getInR(b, bb);
                if (w) {
                    bob[key].within.push(bb);
                }
                if (h) {
                    bob[key].has.push(bb);
                }
            });
        });

        let mostWith = { within: [] } as any;
        let mostHas = { has: [] } as any;

        Object.keys(bob).forEach((k) => {
            if (bob[k].within.length > mostWith.within.length) {
                mostWith = bob[k];
            }

            if (bob[k].has.length > mostHas.has.length) {
                mostHas = bob[k];
            }
        });




        let minWith = { x: Number.MAX_SAFE_INTEGER, y: Number.MAX_SAFE_INTEGER, z: Number.MAX_SAFE_INTEGER };
        let maxWith = { x: 0, y: 0, z: 0 };
        let minHas = { x: Number.MAX_SAFE_INTEGER, y: Number.MAX_SAFE_INTEGER, z: Number.MAX_SAFE_INTEGER };
        let maxHas = { x: 0, y: 0, z: 0 };

        Object.keys(bob).forEach((k) => {
            if (bob[k].within.length === mostWith.within.length) {
                let mins = Nanos.getMins(bob[k].within);
                let maxes = Nanos.getMaxes(bob[k].within);
                if (mins.x < minWith.x) {
                    minWith.x = mins.x;
                }
                if (mins.y < minWith.y) {
                    minWith.y = mins.y;
                }
                if (mins.z < minWith.z) {
                    minWith.z = mins.z;
                }

                if (maxes.x > maxWith.x) {
                    maxWith.x = maxes.x;
                }
                if (maxes.y > maxWith.y) {
                    maxWith.y = maxes.y;
                }
                if (maxes.z > maxWith.z) {
                    maxWith.z = maxes.z;
                }
            }

            if (bob[k].has.length === mostWith.has.length) {
                let mins = Nanos.getMins(bob[k].has);
                let maxes = Nanos.getMaxes(bob[k].has);
                if (mins.x < minHas.x) {
                    minHas.x = mins.x;
                }
                if (mins.y < minHas.y) {
                    minHas.y = mins.y;
                }
                if (mins.z < minHas.z) {
                    minHas.z = mins.z;
                }

                if (maxes.x > maxHas.x) {
                    maxHas.x = maxes.x;
                }
                if (maxes.y > maxHas.y) {
                    maxHas.y = maxes.y;
                }
                if (maxes.z > maxHas.z) {
                    maxHas.z = maxes.z;
                }
            }
        });

        // for (let x = mins.x; x < maxes.x; x++) {
        //     console.log("starting x: " + x);
        //     for (let y = mins.y; y < maxes.y; y++) {
        //         for (let z = mins.z; z < maxes.z; z++) {

        //         }
        //     } 
        // }
        let x = (27759156 - 20636888);
        let y = (39638478 - 34670845);
        let z = (20101045 - 17376549);


        // maxX = 106976519, 106976519
        // maxY = 120677168, 95382542
        // maxZ = 74499123, 99321301

        // minX = -39667998, -68139811
        // minY = -30976870, -30976870
        // minZ = -38634244, -61508102

        return maxSpot.x + "," + maxSpot.y + "," + maxSpot.z;
    }

    public static dist = (base: Bot, other: Bot): number => {
        return Math.abs(base.x - other.x) + Math.abs(base.y - other.y) + Math.abs(base.z - other.z);
    }

    public static getInR = (base: Bot, other: Bot): boolean => {
        let d = Nanos.dist(base, other);
        return d <= base.r;
    }

    public static getMins = (bots: Bot[]): { x: number, y: number, z: number } => {
        let x = Number.MAX_SAFE_INTEGER;
        let y = Number.MAX_SAFE_INTEGER;
        let z = Number.MAX_SAFE_INTEGER;
        bots.forEach((b) => {
            x = b.x < x ? b.x : x;
            z = b.z < z ? b.z : z;
            y = b.y < y ? b.y : y;
        });
        return { x, y, z };
    }


    public static getMaxes = (bots: Bot[]): { x: number, y: number, z: number } => {
        let x = 0;
        let y = 0;
        let z = 0;
        bots.forEach((b) => {
            x = b.x > x ? b.x : x;
            z = b.z > z ? b.z : z;
            y = b.y > y ? b.y : y;
        });
        return { x, y, z };
    }

    public static getMaxR = (bots: Bot[]): Bot => {
        let max = { r: -1 } as any;
        bots.forEach((b) => {
            max = b.r > max.r ? b : max;
        });
        return max;
    }

    public static getProgram = async (): Promise<Bot[]> => {
        let raw: string = await Nanos.fs.readFileAsync("input/23.txt", { encoding: "utf8" });
        let rows = raw.split("\n");

        return rows.map((row) => {
            row = row.replace("pos=<", "");
            let parts = row.split(">,");
            let pos = parts[0].split(",");
            let x = Number(pos[0]);
            let y = Number(pos[1]);
            let z = Number(pos[2]);
            let r = Number(parts[1].split("=")[1]);

            return {
                x, y, z, r
            }
        });
    }


}