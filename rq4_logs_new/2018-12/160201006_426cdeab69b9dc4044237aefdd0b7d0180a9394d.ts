import * as bluebird from 'bluebird';

export class Polymers {
    public static fs = bluebird.Promise.promisifyAll(require('fs'));

    public static bestPolymer = async (): Promise<number> => {
        let chars = 'abcdefghijklmnopqrstuvwxyz'.split('');
        let input = await Polymers.getInput();
        let best = Polymers.collapse(input).length;
        chars.forEach((l) => {
            var regEx = new RegExp(l, "ig")
            let result = Polymers.collapse(input.replace(regEx, '')).length;
            best = result < best ? result : best;
        });

        return best;
    }

    public static polymerLength = async (): Promise<number> => {
        let input = await Polymers.getInput();
        let collapsed = Polymers.collapse(input);

        return collapsed.length;
    }

    public static collapse = (input: string): string => {
        let found = false;
        let flip = false;
        let collapsed = "";
        if (input.length === 1) {
            return input;
        }

        for (let x = 0; x < input.length; x++) {
            if (x + 1 > input.length - 1) {
                collapsed += input[x];
                continue;
            }
            if (flip) {
                flip = false;
                continue;
            }
            if (input[x].toUpperCase() === input[x + 1].toUpperCase() && input[x] !== input[x + 1]) {
                found = true;
                flip = true;
                continue;
            }
            collapsed += input[x];
        }

        if (found) {
            return Polymers.collapse(collapsed);
        }
        return collapsed;
    }

    public static getInput = async (): Promise<string> => {
        let raw: string = await Polymers.fs.readFileAsync("input/dayFive.txt", { encoding: "utf8" });
        return raw.trim().replace("\n", "");
    }
}