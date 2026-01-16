import * as bluebird from 'bluebird';

export interface Light {
    x: number;
    y: number;
    velocity: {
        x: number;
        y: number;
    }
}

export class SkyLights {
    public static fs = bluebird.Promise.promisifyAll(require('fs'));

    public static maxX = 0;
    public static minX = 0;

    public static maxY = 0;
    public static minY = 0;


    public static run = async (): Promise<number> => {
        let data = await SkyLights.getData();


        for (let i = 0; i < 2000000; i++) {
            data = SkyLights.tick(data);
            SkyLights.maxX = 0;
            SkyLights.minX = 0;
            SkyLights.maxY = 0;
            SkyLights.minY = 0;
            let c = SkyLights.convert(data);
            if (SkyLights.maxX - SkyLights.minX < 300 && SkyLights.maxY - SkyLights.minY < 300) {
                await SkyLights.draw(c, i + 1);
            }
            console.log(i + "  x:" + (SkyLights.maxX - SkyLights.minX).toString() + "  y: " + (SkyLights.maxY - SkyLights.minY).toString());
        }
        return 0;
    }

    public static draw = async (values: { [id: string]: number[] }, tick: number): Promise<void> => {


        let strings: string[] = [];
        for (let i = SkyLights.minY; i <= SkyLights.maxY; i++) {
            if (!values[i.toString()]) {
                strings.push(".........................................");
                continue;
            }

            let xs = values[i.toString()].sort((x, y) => {
                return x - y;
            });
            let v = "";

            for (let z = SkyLights.minX; z <= SkyLights.maxX; z++) {
                if (xs.indexOf(z) === -1) {
                    v += ".";
                    continue;
                }
                v += "#";
            }

            strings.push(v);
        }

        await SkyLights.fs.writeFileAsync("input/dayTen/" + tick.toString() + ".txt", strings.join('\n'));

    }

    public static tick = (data: Light[]): Light[] => {
        return data.map((point) => {
            point.x += point.velocity.x;
            point.y += point.velocity.y;
            return point;
        });
    }

    public static convert = (data: Light[]): { [id: string]: number[] } => {
        let ys: { [id: string]: number[] } = {};
        let count = 0;
        data.forEach((point) => {
            if (point.x > SkyLights.maxX) {
                SkyLights.maxX = point.x;
            }
            if (point.x < SkyLights.minX) {
                SkyLights.minX = point.x;
            }
            if (point.y > SkyLights.maxY) {
                SkyLights.maxY = point.y;
            }
            if (point.y < SkyLights.minY) {
                SkyLights.minY = point.y;
            }
            if (!ys[point.y.toString()]) {
                ys[point.y.toString()] = [];
            }

            ys[point.y.toString()].push(point.x);
            count++;
        });

        return ys;
    }


    public static getData = async (): Promise<Light[]> => {
        let raw: string = await SkyLights.fs.readFileAsync("input/dayTen.txt", { encoding: "utf8" });
        let list = raw.split("\n");

        return list.map((row) => {

            let parts = row.split("> velocity=<");
            let position = parts[0].replace("position=<", "").trim().split(",");
            let velocity = parts[1].replace(">", "").trim().split(",");

            let v = {
                x: Number(velocity[0].trim()),
                y: Number(velocity[1].trim())
            };

            let value: Light = {
                x: Number(position[0].trim()),
                y: Number(position[1].trim()),
                velocity: v
            }
            return value;
        });

    }
}