type Node = {
  x:number
  y:number
}

const REJECTED_LIMIT_MULTIPLIER = 100

export function generateRandomNodes(width:number, height:number, nodes:number, nodeRadius:number, nodeInfluenceMultiplier = 2):Array<Node>{
  let nodeList = new Array<Node>();
  let rejectedLimit = nodes * REJECTED_LIMIT_MULTIPLIER;
  let rejected = 0;
  while(rejected < rejectedLimit && nodeList.length < nodes){
    let subject = pickNode(width - nodeRadius * 2, nodeRadius * 2, height - nodeRadius * 2,nodeRadius * 2)
    if(collidesAny(subject,nodeList,nodeRadius * nodeInfluenceMultiplier)){
      rejected++;
    }else{
      nodeList.push(subject);
    }
  }
  if(rejected >= rejectedLimit){
    throw 'Random nodes could not be generated!'
  }
  return nodeList;
}

function pickNode(widthMax:number, widthMin:number, heightMax:number, heightMin:number):Node{
  return {x:(Math.random()*(widthMax-widthMin))+widthMin,y:(Math.random()*(heightMax - heightMin))+heightMin};
}

function collidesAny(node:Node, nodeList:Array<Node>, radius:number):boolean{
  let result = false;
  for(let subject of nodeList){
    if(collides(node,subject,radius)){
      result = true;
    }
  }
  return result;
}

function collides(nodeA:Node, nodeB:Node, radius:number):boolean{
  return within(nodeA.y - radius,nodeB.y,nodeA.y + radius) && within(nodeA.x - radius,nodeB.x,nodeA.x + radius)
}

function within(lowerBound:number, check:number, upperBound:number):boolean{
  return check > lowerBound && check < upperBound
}