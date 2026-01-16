///<reference path='./typings/lodash/lodash.d.ts' />
///<reference path='./utils.ts' />
module Annealing {
    // Algorithm is as follows:
    // 1. Calculate solution
    // 2. Measure quality
    // 3. Determine if quality
    export interface IProblem<DataFrame> {
        energy:(data:DataFrame)=>number
        neighbour:(data:DataFrame)=>DataFrame
        startingTemp:number
        stopTemp:number
        decreaseRate:number
        initState:DataFrame
    }

    function  ifChange(energy:number,newEnergy:number,temperature:number):boolean{
        // This solution is better. Use it
        if (newEnergy<energy){
            return true
        }
        // Choose if we are to use worse solution or not
        var random:number = Math.random()
        var probability:number = Math.pow(Math.E,((energy-newEnergy)/temperature))
        return probability>=random
    }

    export function Solution<DataFrame>(data:IProblem<DataFrame>){
        var state        = data.initState
        var bestState    = data.initState
        var nowTemp      = data.startingTemp
        console.log("Starting optimization with state: ",state," and startingTemp: ",data.startingTemp)
        while(nowTemp>data.stopTemp){
            var newRandomState = data.neighbour(state)
            if(ifChange(data.energy(state),data.energy(newRandomState),nowTemp)){
              state = newRandomState
            }
            nowTemp= nowTemp * (1-data.decreaseRate)
            // Keep track of the best solution found
            if (data.energy(state) < data.energy(bestState)) {
                bestState = state
            }
        }
       console.log("Cooled down. Best solution so far: ",bestState,"with energy: ",data.energy(bestState))
       return bestState
    }
}

module SampleAnnealingSolution {
    var quadratic:Annealing.IProblem<number> = {
        energy:function(x:number):number{
            return Math.abs((110+x)*(213+x)*(245+x)*(143+x))
        },
        neighbour:function(data:number){
            return data + _.sample([-3,-2,-1,1,2,3])
        },
        startingTemp:40000,
        stopTemp:0.4,
        decreaseRate:0.03,
        initState:0
    }
    console.log(Annealing.Solution(quadratic))
}


    //interface quadraticEquasion extends Problem {
    //    fitness:(x1:number,x2:number)=>number
    //}
    //
    //
    //var quadraticEquasion:quadraticEquasion = {
    //    fitness: function(){
    //
    //
    //    }
    //
    //
    //
    //
    //}






