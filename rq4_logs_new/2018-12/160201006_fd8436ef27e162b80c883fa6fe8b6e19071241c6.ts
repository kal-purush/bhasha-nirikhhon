import * as bluebird from 'bluebird';

export interface Star {
    x: number;
    y: number;
    z: number;
    t: number;
}
export class Stars {
    public static fs = bluebird.Promise.promisifyAll(require('fs'));

    public static run = async (): Promise<string> => {
        let sky = await Stars.getProgram();

        let merged = true;

        while (merged) {
            merged = false;
            let ids = Object.keys(sky);

            ids.forEach((id) => {
                if (!sky[id]) {
                    return;
                }

                ids.forEach((i) => {
                    if (!sky[i] || i === id) {
                        return;
                    }
                    let mergable = Stars.Mergable(sky[id], sky[i]);
                    if (!mergable) {
                        return;
                    }

                    sky[id] = sky[id].concat(sky[i]);
                    delete sky[i];
                    merged = true;
                });
            });
        }

        return "Number of groups: " + Object.keys(sky).length.toString();
    }

    public static Mergable = (one: Star[], two: Star[]): boolean => {
        let mergable = false;
        for (let o of one) {
            for (let t of two) {
                let dist = Stars.Distance(o, t);
                if (dist < 4) {
                    mergable = true;
                    break;
                }
            }
            if (mergable) {
                break;
            }
        }

        return mergable;
    }

    public static Distance = (one: Star, two: Star): number => {
        return Math.abs(one.x - two.x) + Math.abs(one.y - two.y) + Math.abs(one.z - two.z) + Math.abs(one.t - two.t);
    }

    public static Key = (star: Star): string => {
        return star.x.toString() + "," + star.y.toString() + "," + star.z.toString() + "," + star.t.toString();
    }

    public static getProgram = async (): Promise<{ [id: string]: Star[] }> => {
        let raw: string = await Stars.fs.readFileAsync("input/25.txt", { encoding: "utf8" });
        let stars: { [id: string]: Star[] } = {};

        let rows = raw.split("\n");
        rows.forEach((row) => {
            let parts = row.trim().split(",");
            let star: Star = {
                x: Number(parts[0]),
                y: Number(parts[1]),
                z: Number(parts[2]),
                t: Number(parts[3])
            }

            stars[Stars.Key(star)] = [star];
        });

        return stars;
    }
}