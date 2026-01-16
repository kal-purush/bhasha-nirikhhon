class MapGenerator {

    constructor() {
        this.createdRooms = [];
    }

    empty(rows, cells) {
        let field = [];
        let item;

        for (let y = 0; y <= rows; y++) {
            field[y] = [];

            for (let x = 0; x <= cells; x++) {
                if (y === 0 || y === rows || x === 0 || x === cells) {
                    item = type.wall;
                } else {
                    item = type.empty;
                }

                field[y][x] = item;
            }
        }

        return field;
    }

    rand(rows, cells) {
        let field = this.empty(rows, cells);
        let item;

        for (let y = 0; y <= rows; y++) {
            field[y] = [];

            for (let x = 0; x <= cells; x++) {
                if (y === 0 || y === rows || x === 0 || x === cells) {
                    item = type.wall;
                } else {
                    item = getRandomInt(0, 3) ? type.empty : type.wall;
                }

                field[y][x] = item;
            }
        }

        // field[1][1] = type.animal;
        // field[1][2] = type.animal;
        // field[1][3] = type.animal;
        // field[1][field[0].length - 2] = type.animal;
        // field[field.length - 2][field[0].length - 2] = type.animal;
        // field[field.length - 2][1] = type.animal;

        return field;
    }

    rooms(rows, cells) {
        this.minRoomSize = {x: 3, y: 3};
        this.maxRoomSize = {x: 8, y: 8};
        this.field = this.empty(rows, cells);
        let cursorRectangle = {x: 0, y: 0};

        let room = this.getRandomRoomSize();
        cursorRectangle = this.drawRoom(cursorRectangle, room);
        console.log(cursorRectangle);

        return this.field;
    }

    drawRoom(cursorRectangle, room) {
        let roomEndX = cursorRectangle.x + room.x;
        let roomEndY = cursorRectangle.y + room.y;

        for (let y = cursorRectangle.y; y <= roomEndY; y++) {
            this.field[y][cursorRectangle.x] = type.wall;
            this.field[y][roomEndX] = type.wall;
        }

        for (let x = cursorRectangle.x; x <= roomEndX; x++) {
            this.field[cursorRectangle.y][x] = type.wall;
            this.field[roomEndY][x] = type.wall;
        }
        let roomsCoordinates = {
            start: {x: cursorRectangle.x, y: cursorRectangle.y},
            end: {x: roomEndX, y: roomEndY}
        };
        this.createdRooms.push(roomsCoordinates);

        return {y: cursorRectangle.y, x: roomEndX};
    }

    getRandomRoomSize() {
        let x = getRandomInt(this.minRoomSize.x, this.maxRoomSize.x);
        let y = getRandomInt(this.minRoomSize.y, this.maxRoomSize.y);

        return {
            x: 5,
            y: 5
        }
    }
}

const mapGenerator = new MapGenerator();