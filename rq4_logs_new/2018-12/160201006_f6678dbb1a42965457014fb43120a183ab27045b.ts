import * as bluebird from 'bluebird';

export interface Light {
    x: number;
    y: number;
    velocity: {
        x: number;
        y: number;
    }
}

export class PowerGrid {
    public static fs = bluebird.Promise.promisifyAll(require('fs'));
    public static maxX = 300;
    public static maxY = 300;
    public static serial = 9798;

    public static findAwesomeBox = async (): Promise<string> => {
        let grid = PowerGrid.buildGrid();

        let max: number = 0;
        let cornerX = -1;
        let cornerY = -1;
        let size = -1;

        for (let y = 0; y < PowerGrid.maxY; y++) {
            for (let x = 0; x < PowerGrid.maxX; x++) {
                for (let s = 1; s < PowerGrid.maxY; s++) {
                    let total = PowerGrid.awesomeBox(x, y, s, grid);
                    //console.log("sum: " + total + " " + x.toString() + ", " + y.toString() + ", " + s.toString());
                    if (total > max) {
                        max = total;
                        size = s;
                        cornerX = x;
                        cornerY = y;
                    }
                }
            }
        }


        return "sum: " + max + " " + (cornerX + 1).toString() + ", " + (cornerY + 1).toString() + ", " + size.toString();
    }


    public static findBox = async (): Promise<string> => {
        let grid = PowerGrid.buildGrid();

        let max: number = 0;
        let centerX = -1;
        let centerY = -1;

        for (let y = 0; y < PowerGrid.maxY; y++) {
            for (let x = 0; x < PowerGrid.maxX; x++) {
                let sum = PowerGrid.addBox(x, y, grid);
                if (sum > max) {
                    max = sum;
                    centerX = x;
                    centerY = y;
                }
            }
        }


        return "sum: " + max + " x: " + (centerX) + " y: " + (centerY);
    }

    public static awesomeBox = (x: number, y: number, size: number, grid: number[][]): number => {
        let sum = 0;

        if (x + size > PowerGrid.maxX - 1 || y + size > PowerGrid.maxY) {
            return 0;
        }

        for (let i = 0; i < size; i++) {
            for (let z = 0; z < size; z++) {
                sum += grid[y + i][x + z];
            }
        }

        return sum;
    }

    public static addBox = (x: number, y: number, grid: number[][]): number => {
        let sum = 0;

        if (x === 0 || y === 0 || y >= PowerGrid.maxY - 1 || x >= PowerGrid.maxX - 1) {
            return sum;
        }

        sum += grid[y][x];
        sum += grid[y][x - 1];
        sum += grid[y][x + 1];

        sum += grid[y + 1][x];
        sum += grid[y + 1][x - 1];
        sum += grid[y + 1][x + 1];


        sum += grid[y - 1][x];
        sum += grid[y - 1][x - 1];
        sum += grid[y - 1][x + 1];

        return sum;
    }

    public static buildGrid = (): number[][] => {
        let ys: number[][] = [];

        for (let y = 1; y <= PowerGrid.maxY; y++) {
            let xs: number[] = [];
            for (let x = 1; x <= PowerGrid.maxX; x++) {
                xs.push(PowerGrid.calPower(x, y));
            }
            ys.push(xs);
        }

        return ys;
    }

    public static calPower = (x: number, y: number): number => {
        let rank = x + 10;
        let power = rank * y;
        power += PowerGrid.serial;
        power *= rank;

        if (power > 99) {
            power = power % 1000;
            let tens = power % 100;
            power -= tens;
            power = power / 100;
        } else {
            power = 0;
        }
        power -= 5;

        return power;
    }


}