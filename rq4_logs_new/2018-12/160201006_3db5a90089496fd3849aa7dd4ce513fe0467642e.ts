import * as bluebird from 'bluebird';

export interface Sample {
    before: number[];
    input: number[];
    after: number[];
}
export class Assembly {
    public static fs = bluebird.Promise.promisifyAll(require('fs'));



    public static addr = (input: number[], opp: number[]): number[] => {
        let x: number[] = [];
        x[0] = input[0];
        x[1] = input[1];
        x[2] = input[2];
        x[3] = input[3];
        x[opp[3]] = input[opp[1]] + input[opp[2]];
        return x;
    }

    public static addi = (input: number[], opp: number[]): number[] => {
        let x: number[] = [];
        x[0] = input[0];
        x[1] = input[1];
        x[2] = input[2];
        x[3] = input[3];
        x[opp[3]] = input[opp[1]] + opp[2];
        return x;
    }

    public static mulr = (input: number[], opp: number[]): number[] => {
        let x: number[] = [];
        x[0] = input[0];
        x[1] = input[1];
        x[2] = input[2];
        x[3] = input[3];
        x[opp[3]] = input[opp[1]] * input[opp[2]];
        return x;
    }


    public static muli = (input: number[], opp: number[]): number[] => {
        let x: number[] = [];
        x[0] = input[0];
        x[1] = input[1];
        x[2] = input[2];
        x[3] = input[3];
        x[opp[3]] = input[opp[1]] * opp[2];
        return x;
    }

    public static banr = (input: number[], opp: number[]): number[] => {
        let x: number[] = [];
        x[0] = input[0];
        x[1] = input[1];
        x[2] = input[2];
        x[3] = input[3];
        x[opp[3]] = input[opp[1]] & input[opp[2]];
        return x;
    }

    public static bani = (input: number[], opp: number[]): number[] => {
        let x: number[] = [];
        x[0] = input[0];
        x[1] = input[1];
        x[2] = input[2];
        x[3] = input[3];
        x[opp[3]] = input[opp[1]] & opp[2];
        return x;
    }

    public static borr = (input: number[], opp: number[]): number[] => {
        let x: number[] = [];
        x[0] = input[0];
        x[1] = input[1];
        x[2] = input[2];
        x[3] = input[3];
        x[opp[3]] = input[opp[1]] | input[opp[2]];
        return x;
    }

    public static bori = (input: number[], opp: number[]): number[] => {
        let x: number[] = [];
        x[0] = input[0];
        x[1] = input[1];
        x[2] = input[2];
        x[3] = input[3];
        x[opp[3]] = input[opp[1]] | opp[2];
        return x;
    }

    public static setr = (input: number[], opp: number[]): number[] => {
        let x: number[] = [];
        x[0] = input[0];
        x[1] = input[1];
        x[2] = input[2];
        x[3] = input[3];
        x[opp[3]] = input[opp[1]];
        return x;
    }

    public static seti = (input: number[], opp: number[]): number[] => {
        let x: number[] = [];
        x[0] = input[0];
        x[1] = input[1];
        x[2] = input[2];
        x[3] = input[3];
        x[opp[3]] = opp[1];
        return x;
    }


    public static gtir = (input: number[], opp: number[]): number[] => {
        let x: number[] = [];
        x[0] = input[0];
        x[1] = input[1];
        x[2] = input[2];
        x[3] = input[3];
        x[opp[3]] = opp[1] > input[opp[2]] ? 1 : 0;
        return x;
    }

    public static gtri = (input: number[], opp: number[]): number[] => {
        let x: number[] = [];
        x[0] = input[0];
        x[1] = input[1];
        x[2] = input[2];
        x[3] = input[3];
        x[opp[3]] = input[opp[1]] > opp[2] ? 1 : 0;
        return x;
    }


    public static gtrr = (input: number[], opp: number[]): number[] => {
        let x: number[] = [];
        x[0] = input[0];
        x[1] = input[1];
        x[2] = input[2];
        x[3] = input[3];
        x[opp[3]] = input[opp[1]] > input[opp[2]] ? 1 : 0;
        return x;
    }

    public static eqir = (input: number[], opp: number[]): number[] => {
        let x: number[] = [];
        x[0] = input[0];
        x[1] = input[1];
        x[2] = input[2];
        x[3] = input[3];
        x[opp[3]] = opp[1] === input[opp[2]] ? 1 : 0;
        return x;
    }

    public static eqri = (input: number[], opp: number[]): number[] => {
        let x: number[] = [];
        x[0] = input[0];
        x[1] = input[1];
        x[2] = input[2];
        x[3] = input[3];
        x[opp[3]] = input[opp[1]] === opp[2] ? 1 : 0;
        return x;
    }

    public static eqrr = (input: number[], opp: number[]): number[] => {
        let x: number[] = [];
        x[0] = input[0];
        x[1] = input[1];
        x[2] = input[2];
        x[3] = input[3];
        x[opp[3]] = input[opp[1]] === input[opp[2]] ? 1 : 0;
        return x;
    }

    public static getProgram = async (): Promise<number[][]> => {
        let raw: string = await Assembly.fs.readFileAsync("input/daySixteen.txt", { encoding: "utf8" });
        let cleaned = raw.split("\n");
        return cleaned.map((row) => {
            row = row.trim();
            return row.split(" ").map((x) => Number(x.trim()));
        });
    }

    public static getCleaned = async (): Promise<Sample[]> => {
        let raw: string = await Assembly.fs.readFileAsync("input/daySixteen.txt", { encoding: "utf8" });
        let cleaned = raw.split("\n");

        let results: Sample[] = [];
        let sample: Sample = {} as any;
        cleaned.forEach((row) => {
            if (!row.trim()) {
                return;
            }

            if (row.startsWith("Before")) {
                row = row.replace("Before: ", "").replace("[", "").replace("]", "").trim();
                let parts = row.split(",").map((x) => Number(x.trim()));
                sample.before = parts;
                return;
            }

            if (row.startsWith("After")) {
                row = row.replace("After: ", "").replace("[", "").replace("]", "").trim();
                let parts = row.split(",").map((x) => Number(x.trim()));
                sample.after = parts;
                results.push(sample);
                sample = {} as any;
                return;
            }

            row = row.trim();
            let parts = row.split(" ").map((x) => Number(x.trim()));
            sample.input = parts;
            return;
        });

        return results;
    }

    public static run = async (): Promise<string> => {
        let input = await Assembly.getProgram();
        let start = [0, 0, 0, 0];
        input.forEach((x) => {
            start = Assembly.numMap[x[0].toString()](start, x);
        })

        return start.join(", ");
    }

    public static countOptions = async (): Promise<number> => {
        let input = await Assembly.getCleaned();
        let blah: { [id: string]: Set<number> } = {};
        let dups = input.filter((sample) => {
            let matched = Object.keys(Assembly.map).filter((opp) => {
                let r = Assembly.map[opp](sample.before, sample.input);
                let e = sample.after;
                return r[0] === e[0] && r[1] === e[1] && r[2] === e[2] && r[3] === e[3];
            });

            matched.forEach((x) => {
                if (!blah[x]) {
                    blah[x] = new Set();
                }
                blah[x].add(sample.input[0]);
            });
            return matched.length >= 3;
        });

        Object.keys(blah).forEach((e) => {
            console.log(e + ": " + [...blah[e]].join(","))
        });

        let final: { [id: string]: string } = {};
        while (Object.keys(final).length < 16) {
            let matches = Object.entries(blah).filter((x) => [...x[1]].length === 1);
            matches.forEach((m) => {
                let key = [...m[1]][0].toString();
                final[key] = m[0];
                Object.entries(blah).forEach((e) => {
                    e[1].delete(Number(key));
                })
            });

        }

        return dups.length;

    }

    public static map: { [id: string]: Function } = {
        "addr": Assembly.addr,
        "addi": Assembly.addi,
        "mulr": Assembly.mulr,
        "muli": Assembly.muli,
        "banr": Assembly.banr,
        "bani": Assembly.bani,
        "borr": Assembly.borr,
        "bori": Assembly.bori,
        "setr": Assembly.setr,
        "seti": Assembly.seti,
        "gtir": Assembly.gtir,
        "gtri": Assembly.gtri,
        "gtrr": Assembly.gtrr,
        "eqir": Assembly.eqir,
        "eqri": Assembly.eqri,
        "eqrr": Assembly.eqrr
    }


    public static numMap: { [id: string]: Function } = {
        "11": Assembly.addr,
        "5": Assembly.addi,
        "1": Assembly.mulr,
        "8": Assembly.muli,
        "4": Assembly.banr,
        "12": Assembly.bani,
        "13": Assembly.borr,
        "9": Assembly.bori,
        "10": Assembly.setr,
        "6": Assembly.seti,
        "7": Assembly.gtir,
        "2": Assembly.gtri,
        "3": Assembly.gtrr,
        "14": Assembly.eqir,
        "0": Assembly.eqri,
        "15": Assembly.eqrr
    }
}