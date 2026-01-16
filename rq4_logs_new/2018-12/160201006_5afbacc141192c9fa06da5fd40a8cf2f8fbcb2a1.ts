import * as bluebird from 'bluebird';

export class FrequencyCounter {
    public static fs = bluebird.Promise.promisifyAll(require('fs'));

    public static findRepeat = async (): Promise<number> => {
        let cleaned = await FrequencyCounter.getCleaned();
        let d: { [id: string]: boolean; } = { 0: true } as any;
        let found = false;
        let current = 0;
        while (!found) {
            cleaned.forEach((val) => {
                if (found) {
                    return;
                }
                current += val;
                if (d[current.toString()]) {
                    found = true;
                    return;
                }
                d[current.toString()] = true;
            });
        }
        return current;
    }

    public static calculate = async (): Promise<number> => {
        let cleaned = await FrequencyCounter.getCleaned();
        return FrequencyCounter.sum(cleaned);
    }

    public static getCleaned = async (): Promise<number[]> => {
        let raw: string = await FrequencyCounter.fs.readFileAsync("input/dayOne.txt", { encoding: "utf8" });
        let cleaned = FrequencyCounter.clean(raw.split("\n"));
        return cleaned;
    }

    public static clean = (input: string[]): number[] => {
        if (!input) {
            return [];
        }

        return input.map((val) => {
            if (val.startsWith("-")) {
                return Number(val.replace("-", "")) * -1;
            }

            return Number(val.replace("+", ""));
        });
    }

    public static sum = (input: number[]): number => {
        if (!input) {
            return 0;
        }

        return input.reduce((total: number, val: number) => {
            return total + val;
        }, 0);
    }
}