///<reference path="Rule.ts"/>
///<reference path="../Solution.ts"/>
///<reference path="../GridSolver.ts"/>
import {Rule} from "./Rule";
import {GraphSolution} from "../Solution";
import {GridSolver} from "../GridSolver";
import {Grid} from "../Grid";

/**
 * Hexagons
 */
export class RequiredVisit implements Rule {
    protected grid: Grid;
    protected solver: GridSolver;
    private visits: number[][];

    constructor(grid: Grid) {
        this.grid = grid;
        this.visits = [];
    }

    public AddNodeVisit(nodeIndex: number) {
        this.visits.push([nodeIndex]);
        return this;
    }

    public AddEdgeVisit(fromIndex: number, toIndex: number) {
        this.visits.push([fromIndex, toIndex]);
        return this;
    }

    Reject(solution: GraphSolution): boolean {
        for(let i = 0; i < this.visits.length; i++) {
            const visit = this.visits[i];

            if(visit.length === 1) {
                if(!solution.NodeIndexInClosedRegion(visit[0], this.grid)) continue;
                if(!solution.NodeIndexOnSolution(visit[0])) return true;
            }

            if(visit.length === 2) {
                if(!solution.EdgeIndexInClosedRegion(visit[0], visit[1], this.grid)) continue;
                if(!solution.EdgeIndexOnSolution(visit[0], visit[1])) return true;
            }
        }

        return false;
    }

    SetSolver(solver:GridSolver) : void {
        this.solver = solver;
    }
}