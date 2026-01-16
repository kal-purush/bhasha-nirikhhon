import * as bluebird from 'bluebird';

export interface Spot {
    x: number;
    y: number;
}
export class WaterClay {
    public static fs = bluebird.Promise.promisifyAll(require('fs'));
    public static MaxY = 0;
    public static MaxX = 0;
    public static offset = 0;
    public static waterStart: Spot = { x: 500 - WaterClay.offset, y: 0 };
    public static Ground: string[][] = [];
    public static GroundTypes = ["#", "~"];
    public static run = async (): Promise<number> => {
        let map = await WaterClay.getData();
        console.log("MaxX: " + WaterClay.MaxX.toString() + " MaxY: " + WaterClay.MaxY.toString());

        let xstring = "".padStart(WaterClay.MaxX + 1 - WaterClay.offset, ".");
        for (let i = 0; i < WaterClay.MaxY + 4; i++) {
            let xs = xstring.split('');
            WaterClay.Ground.push(xs);
        }

        map.forEach((s) => {
            WaterClay.Ground[s.y][s.x - WaterClay.offset] = "#";
        });
        WaterClay.Ground[WaterClay.waterStart.y][WaterClay.waterStart.x] = "+";
        WaterClay.Ground[WaterClay.waterStart.y + 1][WaterClay.waterStart.x] = "|";

        let runWater = true;
        let fillWater = true;
        let count = 0;

        while ((runWater || fillWater) && count < 1000) {
            runWater = WaterClay.runWater();
            fillWater = WaterClay.fillWater();

            console.log(count);
            count++;
        }
        console.log(count);
        await WaterClay.fs.writeFileAsync("input/waterclay.txt", WaterClay.Ground.map((x) => x.map((b) => b === "." ? " " : b).join('')).join("\n"));

        let water = 0;
        let running = 0;
        for (let y = 0; y < WaterClay.MaxY + 1; y++) {
            for (let x = 0; x < WaterClay.Ground[y].length; x++) {
                if (WaterClay.Ground[y][x] === "~") {
                    water++;
                }

                if (WaterClay.Ground[y][x] === "|") {
                    running++;
                }
            }
        }
        console.log("water: " + water.toString() + " running: " + running.toString());
        return water + running;
    }

    public static runWater = (): boolean => {
        let didSomething = false;
        for (let y = 1; y < WaterClay.MaxY + 1; y++) {
            for (let x = 0; x < WaterClay.Ground[y].length; x++) {
                if (WaterClay.Ground[y][x] !== ".") {
                    continue;
                }
                if (WaterClay.Ground[y - 1][x] !== "|") {
                    continue;
                }
                WaterClay.Ground[y][x] = "|";
                didSomething = true;
            }
        }
        return didSomething;
    }

    public static fillWater = (): boolean => {
        let didSomething = false;
        for (let y = 1; y < WaterClay.MaxY + 1; y++) {
            for (let x = 0; x < WaterClay.Ground[y].length; x++) {
                if (WaterClay.Ground[y][x] !== "|") {
                    continue;
                }
                if (WaterClay.Ground[y + 1][x] !== "#" && WaterClay.Ground[y + 1][x] !== "~") {
                    continue;
                }
                let right = -1;
                let left = -1;
                let hole = false;

                for (let lx = x; lx > -1; lx--) {
                    if (WaterClay.Ground[y + 1][lx] === "." || WaterClay.Ground[y + 1][lx] === "|") {
                        hole = true;
                        left = lx;
                        let temp = WaterClay.Ground[y + 1][lx];
                        WaterClay.Ground[y + 1][lx] = "|";
                        if (temp !== WaterClay.Ground[y + 1][lx]) {
                            didSomething = true;
                        }
                        break;
                    }
                    if (WaterClay.Ground[y][lx] === "#") {
                        break;
                    }
                    left = lx;
                }
                for (let rx = x; rx < WaterClay.Ground[y].length; rx++) {
                    if (WaterClay.Ground[y + 1][rx] === "." || WaterClay.Ground[y + 1][rx] === "|") {
                        hole = true;
                        right = rx;
                        WaterClay.Ground[y + 1][rx] = "|";
                        break;
                    }
                    if (WaterClay.Ground[y][rx] === "#") {
                        break;
                    }
                    right = rx;
                }

                for (let xx = left; xx < right + 1; xx++) {
                    if (WaterClay.Ground[y][xx] === "#") {
                        continue;
                    }
                    let b = WaterClay.Ground[y][xx];
                    WaterClay.Ground[y][xx] = hole ? "|" : "~";
                    if (b !== WaterClay.Ground[y][xx]) {
                        didSomething = true;
                    }
                }

            }
        }
        return didSomething;
    }

    public static getData = async (): Promise<Spot[]> => {
        let raw: string = await WaterClay.fs.readFileAsync("input/daySeventeen.txt", { encoding: "utf8" });
        let cleaned = raw.split("\n");
        let spots: Spot[] = [];
        cleaned.forEach((row) => {
            let parts = row.split(", ");
            let firstParts = parts[0].split("=");
            let num = Number(firstParts[1]);
            let val = firstParts[0];

            let secondParts = parts[1].split("=");
            let secondVal = secondParts[0];
            let rangeParts = secondParts[1].split("..");

            let start = Number(rangeParts[0]);
            let end = Number(rangeParts[1]);
            while (start <= end) {
                if (val === "x") {
                    WaterClay.MaxX = num > WaterClay.MaxX ? num : WaterClay.MaxX;
                    WaterClay.MaxY = start > WaterClay.MaxY ? start : WaterClay.MaxY;
                } else {
                    WaterClay.MaxX = start > WaterClay.MaxX ? start : WaterClay.MaxX;
                    WaterClay.MaxY = num > WaterClay.MaxY ? num : WaterClay.MaxY;
                }
                let s = {} as any;
                s[val] = num;
                s[secondVal] = start;
                spots.push(s);
                start++;
            }


        });

        return spots;
    }


}