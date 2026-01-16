import * as bluebird from 'bluebird';

export interface Data {
    children: Data[];
    metaData: number[];
    value: number;
}
export class LocationData {
    public static fs = bluebird.Promise.promisifyAll(require('fs'));



    public static addNodes = async (): Promise<number> => {
        let data = await LocationData.getData();

        let process = (): Data => {
            let datam: Data = { children: [], metaData: [], value: 0 };
            let children = data.shift() || 0;
            let metaNumber = data.shift() || 0;

            while (children > 0) {
                datam.children.push(process())
                children--;
            }

            while (metaNumber > 0) {
                datam.metaData.push(data.shift() || 0);
                metaNumber--;
            }

            if (!datam.children.length) {
                datam.value = datam.metaData.reduce((total, c) => total + c, 0);
                return datam;
            }

            datam.value = datam.metaData.reduce((total, m) => {
                if (m > datam.children.length) {
                    return total;
                }

                total += datam.children[m - 1].value;
                return total;
            }, 0);

            return datam;
        }

        return process().value;
    }

    public static addMeta = async (): Promise<number> => {
        let data = await LocationData.getData();
        let total = 0;

        let process = () => {
            let children = data.shift() || 0;
            let metaNumber = data.shift() || 0;

            while (children > 0) {
                process();
                children--;
            }

            while (metaNumber > 0) {
                total += data.shift() || 0;
                metaNumber--;
            }
        }
        process();
        return total;
    }

    public static getData = async (): Promise<number[]> => {
        let raw: string = await LocationData.fs.readFileAsync("input/dayEight.txt", { encoding: "utf8" });
        let cleaned = raw.split("\n");
        let final: number[] = [];
        cleaned.map((row) => {
            return row.split(" ").map((v) => {
                return Number(v);
            })
        }).forEach((x) => {
            final = final.concat(x);
        })
        return final;
    }

}