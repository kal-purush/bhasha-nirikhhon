export type WorkId = number;

type WorkData = {
    id: WorkId;
    time: number;
    depends?: WorkId[];
}

export class Work {

    readonly id: WorkId;
    readonly time: number;
    readonly depends: WorkId[];

    constructor(data: WorkData) {
        this.id = data.id;
        this.time = data.time;
        this.depends = data.depends || [];
    }
}