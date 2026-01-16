/// <reference path = "Coordinate.ts"/>
/// <reference path = "utils.ts"/>

class
Graph {

  private
  checkNodeDefinitions(ndefs: any []): void {
    setIterate(ndefs, function (nd) {
      assert(nd.name != undefined, "node definition has no name");
      assert(nd.cost != undefined, "node definition has no cost");
      assert(nd.center != undefined, "node definition has no center");
      assert(nd.neighbors != undefined, "node definition has no neighbors");
      });

    var names = { };
    setIterate(ndefs, function (nd) {
      assert(names[nd.name] == undefined,
	"node name " + nd.name + " is multiply defined");
      names[nd.name] = true;
      });
    }


  constructor(ndefs: any []) {

    this.checkNodeDefinitions(ndefs);

    var nodeNames = [ ];
    var nodeEdges = { };
    var nodeCenters = { };
    var nodeCosts = { };

    setIterate(ndefs, function(n) {
      nodeNames.push(n.name);
      nodeCosts[n.name] = n.cost;
      nodeCenters[n.name] = n.center;
      nodeEdges[n.name] = n.neighbors;
      });

    this.nodeNames = nodeNames;
    this.nodeEdges = nodeEdges;
    this.nodeCenters = nodeCenters;
    this.nodeCosts = nodeCosts;
    }

  private nodeNames;
  private nodeCosts;
  private nodeCenters;
  private nodeEdges;
  }
