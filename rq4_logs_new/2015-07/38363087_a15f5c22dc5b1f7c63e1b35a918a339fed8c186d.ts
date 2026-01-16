import VEHICLE = require("Vehicle");

export class Boat extends VEHICLE.Vehicle
{
    constructor (name: string)
    {
        super("BOAT " + name);
    }

    do() : string
    {
        return "Im a boat";
    }
}