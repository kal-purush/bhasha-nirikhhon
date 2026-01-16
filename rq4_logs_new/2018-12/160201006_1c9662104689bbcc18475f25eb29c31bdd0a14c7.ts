import * as bluebird from 'bluebird';

export interface Drink {
    score: number;
    next: Drink;
    elfOne: boolean;
    elfTwo: boolean;
}
export class Drinks {

    public static fs = bluebird.Promise.promisifyAll(require('fs'));
    public static End = 919901;
    public static EndTwo = "51589";

    public static runTwo = async (): Promise<string> => {

        let first: Drink = {
            score: 3,
            next: {} as any,
            elfOne: true,
            elfTwo: false
        };

        let last: Drink = {
            score: 7,
            next: first,
            elfOne: false,
            elfTwo: true
        }
        first.next = last;


        let len = 2;
        let e1 = 3;
        let e2 = 7;
        let stop = Drinks.End + 10;
        let one = first;
        let two = first.next;
        let bob = "";

        while (bob.indexOf(Drinks.EndTwo) < 0) {

            let sum = e1 + e2;
            let parts = sum.toString().split("");
            parts.forEach((v) => {
                let n: Drink = {
                    score: Number(v),
                    next: first,
                    elfOne: false,
                    elfTwo: false
                };
                last.next = n;
                last = n;
                len++;
                bob += v;
            });

            let oneToMove = e1 + 1;
            while (oneToMove > 0) {
                one.elfOne = false;
                one = one.next;
                one.elfOne = true;
                oneToMove--;
                e1 = one.score;
            }
            let twoToMove = e2 + 1;
            while (twoToMove > 0) {
                two.elfTwo = false;
                two = two.next;
                two.elfTwo = true;
                twoToMove--;
                e2 = two.score;
            }


            if (len % 10000 === 0) {
                console.log(len.toString());
            }
        }

        return len.toString();
    }
    public static run = async (): Promise<string> => {

        let first: Drink = {
            score: 3,
            next: {} as any,
            elfOne: true,
            elfTwo: false
        };

        let last: Drink = {
            score: 7,
            next: first,
            elfOne: false,
            elfTwo: true
        }
        first.next = last;


        let len = 2;
        let e1 = 3;
        let e2 = 7;
        let stop = Drinks.End + 10;
        let one = first;
        let two = first.next;
        let bob = "";
        while (len < stop) {

            let sum = e1 + e2;
            let parts = sum.toString().split("");
            parts.forEach((v) => {
                let n: Drink = {
                    score: Number(v),
                    next: first,
                    elfOne: false,
                    elfTwo: false
                };
                last.next = n;
                last = n;
                len++;
            });

            let oneToMove = e1 + 1;
            while (oneToMove > 0) {
                one.elfOne = false;
                one = one.next;
                one.elfOne = true;
                oneToMove--;
                e1 = one.score;
            }
            let twoToMove = e2 + 1;
            while (twoToMove > 0) {
                two.elfTwo = false;
                two = two.next;
                two.elfTwo = true;
                twoToMove--;
                e2 = two.score;
            }
            // let p = first;
            // while (oneToMove > 0 || twoToMove > 0) {
            //     if (p.elfOne && oneToMove > 0) {
            //         p.elfOne = false;
            //         p.next.elfOne = true;
            //         e1 = p.next.score;
            //         oneToMove--;
            //     }

            //     if (p.elfTwo && twoToMove > 0) {
            //         p.elfTwo = false;
            //         p.next.elfTwo = true;
            //         e2 = p.next.score;
            //         twoToMove--;
            //     }
            //     p = p.next;
            // }

            if (len % 10000 === 0) {
                console.log(len.toString());
            }
        }
        let s = "";
        let pr = first;
        let f = Drinks.End - 1;
        for (let i = 0; i < len; i++) {

            if (i > f && s.length < 10) {
                s += pr.score.toString();
            }

            pr = pr.next;
        }
        console.log(s);

        return s;
    }
}