export type ManHours = {
    hours: number,
    minutes: number,
}
export interface IStore {
    settings:{
        manHours: ManHours,
        enableManHours: boolean,
    };
}