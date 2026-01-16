import * as bluebird from 'bluebird';

export interface Step {
    id: string;
    depends: string;
}
export interface MappedSteps {
    [id: string]: string[]
}


export class StepDepends {
    public static fs = bluebird.Promise.promisifyAll(require('fs'));


    public static getHelperTime = async (): Promise<number> => {
        let steps = await StepDepends.getData();
        let mapped = StepDepends.buildDepends(steps);
        let times = StepDepends.buildTime();


        let second = 0;
        let workers: { [id: string]: { t: number, j: string } } = {
            "1": { t: 0, j: "" },
            "2": { t: 0, j: "" },
            "3": { t: 0, j: "" },
            "4": { t: 0, j: "" }
        };
        while (Object.keys(mapped).length) {
            for (let name in workers) {
                if (workers[name].t > 0) {
                    workers[name].t--;
                }
                if (workers[name].t < 1 && workers[name].j) {
                    mapped = StepDepends.removeDepend(mapped, workers[name].j);
                    console.log("worker: " + name + "  finished: " + workers[name].j);
                    workers[name].j = "";
                }
            }

            let next = StepDepends.getNext(mapped).filter((step) => {
                let notRunning = true;
                for (let name in workers) {
                    if (workers[name].j === step) {
                        notRunning = false;
                        break;
                    }
                }
                return notRunning;
            });

            if (!Object.keys(mapped).length) {
                break;
            }
            next.forEach((step) => {
                for (let name in workers) {
                    if (workers[name].j === "") {
                        console.log("worker: " + name + "  starting: " + step);
                        workers[name].t = times[step];
                        workers[name].j = step;

                        return;
                    }
                }
            });

            second++;
        }

        return second;
    }

    public static buildTime = (): { [id: string]: number } => {
        let abc = "abcdefghijklmnopqrstuvwxyz".toUpperCase();
        let array = abc.split("");
        let result: { [id: string]: number } = {};
        array.forEach((v, i) => {
            result[v] = i + 61;
        });
        return result;
    }

    public static getStepOrder = async (): Promise<string> => {
        let steps = await StepDepends.getData();
        let mapped = StepDepends.buildDepends(steps);
        let output = "";

        while (Object.keys(mapped).length) {
            let next = StepDepends.getNext(mapped)[0];
            output += next;
            mapped = StepDepends.removeDepend(mapped, next);
        }

        return output;
    }

    public static removeDepend = (mapped: MappedSteps, id: string): MappedSteps => {
        delete mapped[id];
        for (let key in mapped) {
            mapped[key] = mapped[key].filter((x) => {
                return x !== id;
            });
        }

        return mapped;
    }

    public static getNext = (mapped: MappedSteps): string[] => {
        let nexts = Object.entries(mapped).filter((v) => {
            return v[1].length === 0;
        });

        return nexts.map((val) => {
            return val[0];
        }).sort();
    }

    public static buildDepends = (steps: Step[]): MappedSteps => {
        let mapped: MappedSteps = {};
        let ids = new Set();

        steps.forEach((step) => {
            if (!mapped[step.id]) {
                mapped[step.id] = [];
            }
            mapped[step.id].push(step.depends);
            ids.add(step.depends);
        });

        ids.forEach((id) => {
            if (mapped[id]) {
                return;
            }

            mapped[id] = [];
        });

        return mapped;
    }


    public static getData = async (): Promise<Step[]> => {
        let raw: string = await StepDepends.fs.readFileAsync("input/daySeven.txt", { encoding: "utf8" });
        let rows = raw.split("\n");

        return rows.map((v) => {
            "Step H must be finished before step C can begin."
            let parts = v.split(' must be finished before step ');
            return {
                depends: parts[0].replace("Step ", ""),
                id: parts[1].replace(" can begin.", "")
            }
        });
    }

}