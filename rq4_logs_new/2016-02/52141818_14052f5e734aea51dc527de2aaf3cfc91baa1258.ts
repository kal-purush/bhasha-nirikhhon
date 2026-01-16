///<reference path="Grid.ts"/>
///<reference path="rules/Rule.ts"/>
import {Grid} from "./Grid";
import {Node} from "./Graph";
import {Rule} from "./rules/Rule";
import {last, notPresent} from "./Util";
import {Backtrack} from "./Backtrack";
import {GraphSolution} from "./Solution";

export class GridSolver {
    private grid : Grid;
    private rules : Rule[];
    private startNodes: number[];
    private exitNodes: number[];
    private allowBacktrack: boolean = true;

    constructor(grid: Grid, startNodes: number[], exitNodes: number[], allowBacktrack = true) {
        this.grid = grid;
        this.rules = [];

        this.startNodes = startNodes;
        this.exitNodes = exitNodes

        this.allowBacktrack = allowBacktrack;
    }

    AddRule(rule: Rule) {
        this.rules.push(rule);
        return this;
    }

    OpenRegionContainsExit(s: GraphSolution) {
        const open = s.OpenRegion();
        // If all are open, or we're at the exit, then yeah, we can probably get there.
        if(open < 0) return true;

        const exits = this.exitNodes;

        const cellIndexes = s.GroupedRegions()[open];
        for(let j = 0; j < exits.length; j++) {
            var exitCells = this.grid.CellsBoundingNodeIndex(exits[j]);

            for(let i = 0; i < cellIndexes.length; i++) {
                var cell = cellIndexes[i];
                for(let k = 0; k < exitCells.length; k++) {
                    if(cell === exitCells[j]) return true;
                }
            }
        }

        return false;
    }

    Reject(s: GraphSolution) {
        s.SetRegions(this.grid, this);

        for(let i = 0; i < this.rules.length; i++) {
            if(this.rules[i].Reject(s)) return true;
        }

        if(!this.OpenRegionContainsExit(s)) return true;

        if(!this.allowBacktrack) {
            if(!s.Previous() || !s.Last()) return false;

            return s.Last().Index() - s.Previous().Index() < 0;
        }

        return false;
    }

    Solve(totalToFind: number, start: GraphSolution = null) {
        const firstSolution = start || new GraphSolution(
            [this.grid.Graph().NodeAt(this.startNodes[0])],
            [], []
        );

        var solutions : number[][] = [];
        Backtrack<Node<number>>(
            firstSolution,
            (s: GraphSolution) => this.Reject(s),
            (s: GraphSolution) => s.Last().Index() === this.exitNodes[0],
            (s: GraphSolution) => notPresent(s.RawSolution(), s.Last().Nodes()),
            (s: GraphSolution) => {
                solutions.push(s.RawSolution().map(t => t.Index()));
                if(solutions.length % 100000 === 0) console.log(`Up to ${solutions.length} distinct solutions`);
            },
            totalToFind
        );

        return solutions;
    }

    IsExit(node: Node<number>) {
        const index = node.Index();
        for(let i = 0; i < this.exitNodes.length; i++) {
            if(index === this.exitNodes[i]) return true;
        }

        return false;
    }
}