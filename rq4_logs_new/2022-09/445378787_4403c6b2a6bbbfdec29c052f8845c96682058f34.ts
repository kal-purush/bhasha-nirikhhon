import {BoardNode} from "../data-models";
import {shuffle} from "lodash";
import {lineCircle, lineLine} from "intersects";


type Connection = Array<string>;

export function generateConnections(nodes:Array<BoardNode>, connections:number, nodeRadius, nodeInfluenceMultiplier):Array<Connection>{
  let nodeMap = {};
  nodes.forEach(node => nodeMap[node.label] = node);
  let combos = shuffle(getAllCombinations(nodes));
  let result = [];
  let i = 0;
  while(result.length < connections && i < combos.length){

    i--
  }
  return null;
}

export function getAllCombinations(nodes:Array<BoardNode>):Array<Array<string>>{
  return nodes.flatMap(
    (v, i) => nodes.slice(i+1).map( w => [v.label,w.label] )
  );
}

function connectionCollides(subject:Array<string>,nodeMap:Record<string,BoardNode>,connections:Array<Array<string>>,nodeRadius){
  let start = nodeMap[subject[0]];
  let end = nodeMap[subject[1]];
  for(let node of Object.values(nodeMap)){
    if(connectionCollidesNode(start,end,node,nodeRadius)){
      return true;
    }
  }
  for(let connection of connections){
    if(connectionCollidesConnection(start,end,nodeMap[connection[0]],nodeMap[connection[1]])){
      return true;
    }
  }
  return false;
}

function connectionCollidesNode(connectionStart:BoardNode,connectionEnd:BoardNode,node:BoardNode,nodeRadius:number):boolean{
  if(connectionStart.label === node.label || connectionEnd.label === node.label){
    return false;
  }
  return lineCircle(connectionStart.x, connectionStart.y, connectionEnd.x, connectionEnd.y, node.x, node.y,nodeRadius);
}

function connectionCollidesConnection(aStart:BoardNode,aEnd:BoardNode,bStart:BoardNode,bEnd:BoardNode):boolean{
  return lineLine(aStart.x, aStart.y, aEnd.x, aEnd.y, bStart.x, bStart.y, bEnd.x, bEnd.y, 1, 1);
}