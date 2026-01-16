import { Facing } from "../robot/Robot";
import * as _ from 'lodash';

export interface RobotCoordinates {
    x: number
    y: number
    f: Facing
}

export const serializePlaceCommand = (command: string): RobotCoordinates => {
    const coordinates: Array<string> = command.split(" ").splice(1);
    const facing = Object.values(Facing).indexOf(_.capitalize(coordinates[2]));
    // const facing = Facing[coordinates[2]]
    return { x: Number(coordinates[0]), y: Number(coordinates[1]), f: facing }
}