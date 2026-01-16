import * as bluebird from 'bluebird';

export interface Spot {
    x: number;
    y: number;
}
export class Woods {
    public static fs = bluebird.Promise.promisifyAll(require('fs'));
    public static run = async (): Promise<number> => {
        let numbers = [226806,
            220528,
            219849,
            213057,
            213462,
            205700,
            207088,
            203211,
            205542,
            200466,
            201264,
            196144,
            196020,
            192075,
            193452,
            191425,
            194884,
            194110,
            196803,
            193428,
            199593,
            201348,
            208603,
            210672,
            217634,
            217462,
            223728,
            224553];
        let len = numbers.length;
        let index = 0;
        for (let i = 476; i < 1000000000; i++) {
            index++;
            if (index < len) {
                index = 0;
            }
        }
        let n = 1000000000 - 475;
        console.log(n % len);
        return numbers[21];
    }
    public static runFields = async (): Promise<number> => {
        let field = await Woods.getData();
        let minutes = 0;
        let final = "";
        let wood = 0;
        let lumberyard = 0;
        let fields = 0;
        while (minutes < 1000) {
            wood = 0;
            lumberyard = 0;
            fields = 0;
            let newField: string[][] = [];
            for (let y = 0; y < field.length; y++) {
                newField.push([]);
                for (let x = 0; x < field[y].length; x++) {
                    let neighbors = Woods.getNeighbors(x, y, field);
                    newField[y][x] = field[y][x];
                    if (field[y][x] === ".") {
                        fields++;
                        if (neighbors["woods"] > 2) {
                            newField[y][x] = "|";
                            continue;
                        }
                    }
                    if (field[y][x] === "|") {
                        wood++;
                        if (neighbors["lumberyard"] > 2) {
                            newField[y][x] = "#";
                            continue;
                        }
                    }
                    if (field[y][x] === "#") {
                        lumberyard++;
                        if (neighbors["woods"] < 1 || neighbors["lumberyard"] < 1) {
                            newField[y][x] = ".";
                            continue;
                        }
                    }
                }
            }
            field = newField;
            minutes++;
            let result = wood * lumberyard;
            //console.log("minutes: " + minutes + " fields: " + fields.toString() + " wood: " + wood.toString() + " lumberyard: " + lumberyard.toString() + " result: " + result.toString());
            console.log(minutes + "," + fields.toString() + "," + wood.toString() + "," + lumberyard.toString() + "," + result.toString());

            // final += "minute: " + minutes.toString() + "\n";
            // final += field.map((y) => {
            //     return y.join("");
            //  }).join("\n");
            //  final += "\n";
            // final += "\n";
        }

        //await Woods.fs.writeFileAsync("input/woods.txt", final);
        wood = 0;
        lumberyard = 0;
        fields = 0;
        for (let y = 0; y < field.length; y++) {
            for (let x = 0; x < field[y].length; x++) {
                if (field[y][x] === ".") {
                    fields++;
                }
                if (field[y][x] === "|") {
                    wood++;
                }
                if (field[y][x] === "#") {
                    lumberyard++;
                }
            }
        }
        let result = wood * lumberyard;
        console.log("fields: " + fields.toString() + " wood: " + wood.toString() + " lumberyard: " + lumberyard.toString() + " result: " + result.toString());
        return result;
    }


    public static getNeighbors = (x: number, y: number, field: string[][]): { [id: string]: number } => {
        let results: { [id: string]: number } = {
            "open": 0,
            "woods": 0,
            "lumberyard": 0
        };

        if (field[y - 1] && field[y - 1][x - 1]) {
            let val = field[y - 1][x - 1];
            results["open"] += val === "." ? 1 : 0;
            results["woods"] += val === "|" ? 1 : 0;
            results["lumberyard"] += val === "#" ? 1 : 0;
        }
        if (field[y - 1] && field[y - 1][x]) {
            let val = field[y - 1][x];
            results["open"] += val === "." ? 1 : 0;
            results["woods"] += val === "|" ? 1 : 0;
            results["lumberyard"] += val === "#" ? 1 : 0;
        }
        if (field[y - 1] && field[y - 1][x + 1]) {
            let val = field[y - 1][x + 1];
            results["open"] += val === "." ? 1 : 0;
            results["woods"] += val === "|" ? 1 : 0;
            results["lumberyard"] += val === "#" ? 1 : 0;
        }
        if (field[y + 1] && field[y + 1][x - 1]) {
            let val = field[y + 1][x - 1];
            results["open"] += val === "." ? 1 : 0;
            results["woods"] += val === "|" ? 1 : 0;
            results["lumberyard"] += val === "#" ? 1 : 0;
        }
        if (field[y + 1] && field[y + 1][x]) {
            let val = field[y + 1][x];
            results["open"] += val === "." ? 1 : 0;
            results["woods"] += val === "|" ? 1 : 0;
            results["lumberyard"] += val === "#" ? 1 : 0;
        }
        if (field[y + 1] && field[y + 1][x + 1]) {
            let val = field[y + 1][x + 1];
            results["open"] += val === "." ? 1 : 0;
            results["woods"] += val === "|" ? 1 : 0;
            results["lumberyard"] += val === "#" ? 1 : 0;
        }
        if (field[y][x - 1]) {
            let val = field[y][x - 1];
            results["open"] += val === "." ? 1 : 0;
            results["woods"] += val === "|" ? 1 : 0;
            results["lumberyard"] += val === "#" ? 1 : 0;
        }
        if (field[y][x + 1]) {
            let val = field[y][x + 1];
            results["open"] += val === "." ? 1 : 0;
            results["woods"] += val === "|" ? 1 : 0;
            results["lumberyard"] += val === "#" ? 1 : 0;
        }

        return results;
    }

    public static getData = async (): Promise<string[][]> => {
        let raw: string = await Woods.fs.readFileAsync("input/dayEighteen.txt", { encoding: "utf8" });
        let cleaned = raw.split("\n");
        return cleaned.map((y) => {
            return y.split('');
        });
    }
}