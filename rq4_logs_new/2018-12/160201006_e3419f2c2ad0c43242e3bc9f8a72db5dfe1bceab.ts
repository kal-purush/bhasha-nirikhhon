import * as bluebird from 'bluebird';


export interface Route {
    path: string[];
    branches: Route[];
}

export interface Room {
    x: number;
    y: number;
}
export class Mapping {
    public static fs = bluebird.Promise.promisifyAll(require('fs'));

    public static raw: string[];

    public static run = async (): Promise<string> => {
        let routes = await Mapping.getProgram();
        let maxX = Number.MIN_SAFE_INTEGER;
        let maxY = Number.MIN_SAFE_INTEGER;
        let minX = Number.MAX_SAFE_INTEGER;
        let minY = Number.MAX_SAFE_INTEGER;

        routes.forEach((x) => {
            maxX = x.x > maxX ? x.x : maxX;
            minX = x.x < minX ? x.x : minX;

            maxY = x.y > maxY ? x.y : maxY;
            minY = x.y < minY ? x.y : minY;
        });



        return "";
    }

    public static walk = (start: Room): Room[] => {
        let rooms: Room[] = [];
        let x = Mapping.raw.shift();
        while (x) {
            if (x === "$") {
                break;
            }

            if (x === ")") {
                break;
            }
            if (x === "|") {
                break;
            }
            if (x === "(") {
                Mapping.raw.unshift("(");
                break;
            }

            let newRoom: Room = rooms.length === 0 ? { x: start.x, y: start.y } : { x: rooms[rooms.length - 1].x, y: rooms[rooms.length - 1].y };

            switch (x) {
                case "N":
                    newRoom.y--;
                    rooms.push(newRoom);
                    break;
                case "S":
                    newRoom.y++;
                    rooms.push(newRoom);
                    break;
                case "E":
                    newRoom.x++;
                    rooms.push(newRoom);
                    break;
                case "W":
                    newRoom.x--;
                    rooms.push(newRoom);
                    break;
            }
            x = Mapping.raw.shift();
        }

        return rooms;
    }

    public static getProgram = async (): Promise<Room[]> => {
        let raw: string = await Mapping.fs.readFileAsync("input/20.txt", { encoding: "utf8" });
        Mapping.raw = raw.split("");
        Mapping.raw.shift();
        let rooms: Room[] = [];
        rooms.push({ x: 0, y: 0 });
        let branches: Room[] = [];
        branches.push(rooms[0]);
        while (Mapping.raw.length > 0) {
            if (Mapping.raw[0] === ")") {
                branches.pop();
            }
            if (Mapping.raw[0] === "(") {
                Mapping.raw.shift();
                let newRooms = Mapping.walk(branches[branches.length - 1]);
                if (newRooms.length > 0) {
                    branches.push(newRooms[newRooms.length - 1]);
                }
                rooms = rooms.concat(newRooms);
                continue;
            }
            let newRooms = Mapping.walk(branches[branches.length - 1]);
            rooms = rooms.concat(newRooms);
            branches.push(rooms[rooms.length - 1]);
        }
        return rooms;
    }


}