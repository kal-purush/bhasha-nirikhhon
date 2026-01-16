import * as bluebird from 'bluebird';

export interface Player {
    id: number;
    score: number;
    next: Player;
}

export interface Marble {
    value: number;
    left: Marble;
    right: Marble;
}

export class Marbles {
    public static players = 459;
    public static maxMarble = 72103 * 100;

    public static fs = bluebird.Promise.promisifyAll(require('fs'));



    public static getScore = async (): Promise<number> => {
        let players = Marbles.getPlayers();

        let currentMarble: Marble = {
            value: 0,
            left: {} as any,
            right: {} as any
        };
        currentMarble.left = currentMarble;
        currentMarble.right = currentMarble;
        let v = 1;
        let currentPlayer = players;

        while (v < Marbles.maxMarble) {
            if (v % 23 === 0) {
                currentPlayer.score += v;
                for (let i = 0; i < 7; i++) {
                    currentMarble = currentMarble.left;
                }
                currentPlayer.score += currentMarble.value;
                let next = currentMarble.right;
                next.left = currentMarble.left;
                currentMarble.left.right = next;
                currentMarble = next;
                currentPlayer = currentPlayer.next;
                v++;
                continue;
            }

            let newMarble: Marble = {
                value: v,
                left: {} as any,
                right: {} as any
            }
            let nextM = currentMarble.right;
            let secondM = nextM.right;
            nextM.right = newMarble;
            newMarble.left = nextM;
            newMarble.right = secondM;
            secondM.left = newMarble;

            currentMarble = newMarble;
            currentPlayer = currentPlayer.next;
            v++;
        }

        let highest = 0;

        for (let i = 0; i < Marbles.players; i++) {
            if (currentPlayer.score > highest) {
                highest = currentPlayer.score;
            }
            currentPlayer = currentPlayer.next;
        }

        return highest;
    }

    public static getPlayers = (): Player => {

        let players: Player[] = [];
        for (let i = 0; i < Marbles.players; i++) {
            players.push({
                id: i,
                score: 0,
                next: {} as any
            });
        }
        let playerFirst = players[0];
        let previous = playerFirst;
        players.shift();
        players.forEach((p) => {
            previous.next = p;
            previous = p;
        });

        previous.next = playerFirst;

        return playerFirst;
    }

}