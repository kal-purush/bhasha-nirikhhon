import {generateRandomNodesBest} from "./node-generator.util";
import {generateRandomConnections} from "./connection-generator.util";

export function generateMap(width:number,height:number,nodes:number,nodeRadius:number,connections:number){
  let resolvedNodes = generateRandomNodesBest(width,height,nodes,nodeRadius);
  let resolvedConnections = generateRandomConnections(resolvedNodes,connections,nodeRadius)
  return {nodes:resolvedNodes,connections:resolvedConnections};
}