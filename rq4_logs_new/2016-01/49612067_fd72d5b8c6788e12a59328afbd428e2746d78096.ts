module EngineM {
    export interface IEntity {
        update: Function;
        draw: Function;
        center: IPoint;
        width: number;
        height: number;
    }
}