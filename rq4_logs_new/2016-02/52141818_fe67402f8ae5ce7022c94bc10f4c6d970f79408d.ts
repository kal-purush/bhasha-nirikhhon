/// <reference path="Graph.ts" />
import {Graph, Node} from "./Graph";
import {clonePush} from "./Util";

export var totalInBt = 0;

export function Backtrack<T>(
    start: T,
    reject: (s: T[]) => boolean,
    accept: (s: T[]) => boolean,
    choices: (s: T[]) => T[],
    output: (s: T[]) => void
)
{
    var bt = function(s: T[]) {
        totalInBt++;
        if(reject(s)) return false;
        if(accept(s)) {
            output(s);
            return true;
        }

        var moves = choices(s);
        for(var i = 0; i < moves.length; i++) {
            bt(clonePush(s, moves[i]));
        }
    };

    bt([start]);
}