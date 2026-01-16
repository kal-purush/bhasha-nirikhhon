package com.cubeproblem.resources;

import com.cubeproblem.domain.Cube;
import com.cubeproblem.domain.CubeIteration;
import com.cubeproblem.domain.CubeOperation;
import com.cubeproblem.domain.CubeProblem;
import com.cubeproblem.domain.IterationResult;
import com.cubeproblem.domain.Solution;

import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;

@Path("cubeproblem")
public class CubeProblemResource {

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getSolution(@Valid CubeProblem cubeProblem) {

        List<Solution> solutions = new ArrayList<>();
        IterationResult iterationResult = new IterationResult();

        try {

            for (CubeIteration cubeIteration: cubeProblem.getIterations()) {

                Cube cube = new Cube(cubeIteration.getN());

                for (CubeOperation cubeOperation: cubeIteration.getCubeOperations()) {

                    try{

                        String[] operation = cubeOperation.getOperation().split(" ");
                        Solution solution = new Solution();
                        solution.setOperation(cubeOperation.getOperation());

                        switch(operation[0]){
                            case "UPDATE":
                                if(operation.length<5 || operation.length>5){
                                    solution.setResult("UPDATE: Number of parameters invalid");
                                } else {
                                    cube.update(Integer.parseInt(operation[1]),Integer.parseInt(operation[2]),Integer.parseInt(operation[3]), Integer.parseInt(operation[4]));
                                    solution.setResult("updated");
                                }
                                break;
                            case "QUERY":
                                if(operation.length<7 || operation.length>7){
                                    solution.setResult("QUERY: Number of parameters invalid");
                                } else {
                                    solution.setResult(cube.query(Integer.parseInt(operation[1]), Integer.parseInt(operation[2]), Integer.parseInt(operation[3]), Integer.parseInt(operation[4]), Integer.parseInt(operation[5]), Integer.parseInt(operation[6])));
                                }
                                break;
                            default:
                                solution.setResult("Invalid Operation");
                        }

                        solutions.add(solution);

                    } catch(NumberFormatException nfe){
                        System.out.println("Invalid data");
                    }

                }

                iterationResult.setSolutions(solutions);

            }

        }catch (Exception e){
            e.printStackTrace();
        }

        return Response.ok(iterationResult).build();

    }

}