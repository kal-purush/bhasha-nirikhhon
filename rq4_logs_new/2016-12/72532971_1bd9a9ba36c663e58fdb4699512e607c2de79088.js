/* A Queue object for queue-like functionality over JavaScript arrays. */
var Queue = function() {
    this.items = [];
};

Queue.prototype.enqueue = function(obj) {
    this.items.push(obj);
};

Queue.prototype.dequeue = function() {
    return this.items.shift();
};

Queue.prototype.isEmpty = function() {
    return this.items.length === 0;
};

/*
 * Performs a breadth-first search on a graph
 * @param {array} graph - Graph, represented as adjacency lists.
 * @param {number} source - The index of the source vertex.
 * @returns {array} Array of objects describing each vertex, like
 *     [{distance: _, predecessor: _ }]
 */
function doBFS(graph, source) {
    let bfsInfo = {};

    for (let key in graph) {
      bfsInfo[key] = {
	        distance: null,
	        predecessor: null };
    }

    bfsInfo[source].distance = 0;

    let queue = new Queue();
    queue.enqueue(source);

    while(!queue.isEmpty()){
      let u  = queue.dequeue();
      for (let link in graph[u]) {
        let temp = graph[u][link];
        if(bfsInfo[temp].distance === null){
          bfsInfo[temp].distance = bfsInfo[u].distance + 1;
          bfsInfo[temp].predecessor = u;
          queue.enqueue(temp);
        }
      }
    }

    return bfsInfo;
};

function getGraphDiameter(nodesAdjList, source) {
  source = source || Object.keys(nodesAdjList)[0];

  let bfsInfo = doBFS(nodesAdjList, source);
  let diameter = {
    nodeA: '',
    nodeB: '',
  };
  let maxDistance = {
    name: '',
    distance: 0,
  };

  for (let node in bfsInfo) {
    if (bfsInfo[node].distance > maxDistance.distance) {
      maxDistance.name = node;
      maxDistance.distance = bfsInfo[node].distance;
    }
  }

  diameter.nodeA = maxDistance.name;

  let bfsInfo2 = doBFS(nodesAdjList, maxDistance.name);

  let maxDistance2 = {
    name: '',
    distance: 0,
  };

  for (let node in bfsInfo2) {
    if (bfsInfo2[node].distance > maxDistance2.distance) {
      maxDistance2.name = node;
      maxDistance2.distance = bfsInfo2[node].distance;
    }
  }

  diameter.nodeB = maxDistance2.name;

  return diameter;
}

export default getGraphDiameter;


// console.log(JSON.stringify(bfsInfo,null,2));

// var adjList = [
//     [1],
//     [0, 4, 5],
//     [3, 4, 5],
//     [2, 6],
//     [1, 2],
//     [1, 2, 6],
//     [3, 5],
//     []
//     ];
// var bfsInfo = doBFS(adjList, 3);
// for (var i = 0; i < adjList.length; i++) {
//     println("vertex " + i + ": distance = " + bfsInfo[i].distance + ", predecessor = " + bfsInfo[i].predecessor);
// }