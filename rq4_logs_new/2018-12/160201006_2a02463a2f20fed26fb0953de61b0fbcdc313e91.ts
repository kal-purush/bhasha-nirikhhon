import * as bluebird from 'bluebird';


export interface Rail {
    velocity: { x: number, y: number };
    line: string;
    cart: {
        velocity: { x: number, y: number },
        turn: number,
        tick: number,
        id: number
    } | null;
}

export class Mine {
    public static fs = bluebird.Promise.promisifyAll(require('fs'));
    public static cartDic: { [id: string]: { x: number, y: number } } = {};
    public static findCrash = async (): Promise<string> => {
        let crash = null as any;
        let mine = await Mine.getMine();
        let tick = 0;
        let output = "";
        while (Object.keys(Mine.cartDic).length !== 1) {
            tick++;



            Mine.tick(tick, mine);

            // let m = mine.map((y) => {
            //     let r = y.map((x) => {
            //         if (!x) {
            //             return " ";
            //         }
            //         return x.cart ? x.cart.id.toString() : x.line;
            //     })
            //     return r.join("");
            // });
            // m.push(" ")
            // m.push(" ");
            // output += m.join("\n");

        }
        // await Mine.fs.writeFileAsync("input/MineOut.txt", output);

        return "tick: " + tick.toString() + " - id: " + Object.entries(Mine.cartDic)[0][0] + "  " + Object.entries(Mine.cartDic)[0][1].x.toString() + "," + Object.entries(Mine.cartDic)[0][1].y.toString();
    }

    public static tick = (tick: number, mine: Rail[][]): { x: number, y: number } | null => {
        let crash = null;
        for (let y = 0; y < mine.length; y++) {
            let yr = mine[y];
            for (let x = 0; x < yr.length; x++) {
                let spot = yr[x];
                if (!spot) {
                    continue;
                }

                if (!spot.cart || spot.cart.tick === tick) {
                    continue;
                }
                //138,57
                //138,57
                //138,57
                spot.cart.tick = tick;


                let newX = x + spot.cart.velocity.x;
                let newY = y + spot.cart.velocity.y;


                // if (!mine[newY] || !mine[newY][newX]) {
                //     if (spot.line === "/") {
                //         if (spot.cart.velocity.x > 0) {
                //             newX = x;
                //             newY = y - 1;
                //         } else if (spot.cart.velocity.x < 0) {
                //             newX = x;
                //             newY = y + 1;
                //         } else if (spot.cart.velocity.y > 0) {
                //             newX = x - 1;
                //             newY = y;
                //         } else {
                //             newX = x + 1;
                //             newY = y;
                //         }
                //     } else {
                //         // "\"
                //         if (spot.cart.velocity.x > 0) {
                //             newX = x;
                //             newY = y + 1;
                //         } else if (spot.cart.velocity.x < 0) {
                //             newX = x;
                //             newY = y - 1;
                //         } else if (spot.cart.velocity.y > 0) {
                //             newX = x + 1;
                //             newY = y;
                //         } else {
                //             newX = x - 1;
                //             newY = y;
                //         }
                //     }
                // }

                if (mine[newY][newX].cart) {

                    delete Mine.cartDic[spot.cart.id.toString()];
                    let blah = mine[newY][newX].cart || { id: -1 };
                    console.log("killing: " + spot.cart.id.toString() + " and " + blah.id);
                    delete Mine.cartDic[blah.id.toString()];
                    mine[newY][newX].cart = null;
                    spot.cart = null;
                    continue;
                }


                let newV = mine[newY][newX].velocity;
                let newLine = mine[newY][newX].line;
                if (newV.x === newV.y) {
                    if (spot.cart.turn === 1) {
                        if (spot.cart.velocity.x > 0) {
                            spot.cart.velocity = { y: -1, x: 0 };
                        } else if (spot.cart.velocity.x < 0) {
                            spot.cart.velocity = { y: 1, x: 0 };
                        } else if (spot.cart.velocity.y > 0) {
                            spot.cart.velocity = { x: 1, y: 0 };
                        } else {//else if (spot.cart.velocity.y < 0) {
                            spot.cart.velocity = { x: -1, y: 0 };
                        }
                    }
                    if (spot.cart.turn === 3) {
                        if (spot.cart.velocity.x > 0) {
                            spot.cart.velocity = { y: 1, x: 0 };
                        } else if (spot.cart.velocity.x < 0) {
                            spot.cart.velocity = { y: -1, x: 0 };
                        } else if (spot.cart.velocity.y > 0) {
                            spot.cart.velocity = { x: -1, y: 0 };
                        } else {//else if (spot.cart.velocity.y < 0) {
                            spot.cart.velocity = { x: 1, y: 0 };
                        }
                    }

                    spot.cart.turn = spot.cart.turn === 3 ? 1 : spot.cart.turn + 1;
                    mine[newY][newX].cart = spot.cart;
                    Mine.cartDic[spot.cart.id.toString()] = { x: newX, y: newY };
                    spot.cart = null;
                    continue;
                }

                if (newLine === "/") {
                    if (spot.cart.velocity.x) {
                        spot.cart.velocity.y = spot.cart.velocity.x > 0 ? -1 : 1;
                        spot.cart.velocity.x = 0;
                        spot.line = spot.line ? spot.line : "?";
                    } else {
                        spot.cart.velocity.x = spot.cart.velocity.y > 0 ? -1 : 1;
                        spot.cart.velocity.y = 0;
                        spot.line = spot.line ? spot.line : "?";
                    }
                    mine[newY][newX].cart = spot.cart;
                    Mine.cartDic[spot.cart.id.toString()] = { x: newX, y: newY };
                    spot.cart = null;

                    continue;
                }

                if (newLine === "\\") {
                    if (spot.cart.velocity.x) {
                        spot.cart.velocity.y = spot.cart.velocity.x > 0 ? 1 : -1;
                        spot.cart.velocity.x = 0;
                        spot.line = spot.line ? spot.line : "?";
                    } else {
                        spot.cart.velocity.x = spot.cart.velocity.y > 0 ? 1 : -1;
                        spot.cart.velocity.y = 0;
                        spot.line = spot.line ? spot.line : "?";
                    }
                    mine[newY][newX].cart = spot.cart;
                    Mine.cartDic[spot.cart.id.toString()] = { x: newX, y: newY };
                    spot.cart = null;

                    continue;
                }


                mine[newY][newX].cart = spot.cart;
                Mine.cartDic[spot.cart.id.toString()] = { x: newX, y: newY };
                spot.cart = null;
                spot.line = spot.line ? spot.line : "?";

            }
        }


        return null;
    }

    public static getMine = async (): Promise<Rail[][]> => {
        let raw: string = await Mine.fs.readFileAsync("input/dayThirteen.txt", { encoding: "utf8" });
        let split = raw.split("\n");
        let carts = 0;
        return split.map((line) => {
            let rails = line.split('');
            return rails.map((l): Rail => {
                let rail = null as any;
                switch (l.trim()) {
                    case "|":
                        rail = {
                            velocity: { x: 0, y: 1 },
                            cart: null
                        }
                        break;
                    case "-":
                        rail = {
                            velocity: { x: 1, y: 0 },
                            cart: null
                        }
                        break;
                    case "\\":
                        rail = {
                            velocity: { x: -1, y: 1 },
                            cart: null
                        }
                        break;
                    case "/":
                        rail = {
                            velocity: { x: -1, y: 1 },
                            cart: null
                        }
                        break;
                    case "+":
                        rail = {
                            velocity: { x: -1, y: -1 },
                            cart: null
                        }
                        break;
                    case "v":
                        rail = {
                            velocity: { x: 0, y: 1 },
                            cart: {
                                velocity: {
                                    y: 1, x: 0
                                },
                                turn: 1, tick: 0
                            }
                        }
                        break;
                    case "^":
                        rail = {
                            velocity: { x: 0, y: 1 },
                            cart: {
                                velocity: {
                                    y: -1, x: 0
                                },
                                turn: 1, tick: 0
                            }
                        }
                        break;
                    case "<":
                        rail = {
                            velocity: { x: 1, y: 0 },
                            cart: {
                                velocity: {
                                    y: 0, x: -1
                                }, turn: 1, tick: 0
                            }
                        }
                        break;
                    case ">":
                        rail = {
                            velocity: { x: 1, y: 0 },
                            cart: {
                                velocity: {
                                    y: 0, x: 1
                                }, turn: 1, tick: 0
                            }
                        }
                        break;
                    default:
                        rail = null
                        break;
                }
                if (rail) {
                    rail.line = l.trim();
                }
                if (rail && rail.cart) {
                    rail.cart.id = carts;
                    Mine.cartDic[carts.toString()] = { x: -1, y: -1 };
                    carts++;
                }
                return rail;
            });
        })
    }
}