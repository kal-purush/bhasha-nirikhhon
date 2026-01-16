function Graph(){
  this.map = {}
  this.visited = {};

  //used an array for simplicity
  this.queue = [];

  this.connect = function(val1,val2){
    var node1 = this.find(val1);
    var node2 = this.find(val2);

    if(node1 == false){
      node1 = new Node(val1);
      this.map[val1] = node1
    }

    if(node2 == false){
      node2 = new Node(val2);
      this.map[val2] = node2;
    }

    node1['connections'][val2] = true;
  }

  this.find = function(value){
    var map = this.map;

    if(value in map){
      return map[value];
    }else{
      return false;
    }

  }

  //Depth First Search
  this.hasPath = function(val1,val2){
    //make sure both nodes are in the map
    if(!this.find(val1) || !this.find(val2)){
      console.log('here',val1,val2);
      return false;
    }

    var node = this.map[val1];

    //break case
    if(node['value'] == val2){
      return true;
    }else{
      //make sure node is visited
      this.visited[val1] = true;
    }


    for(key in node['connections']){

      if(!(key in this.visited)){
        // return false;
        var result = this.hasPath(key, val2);

        if(result == true){
          return true;
        }

      }

    }

    //none of the children had a path to the destination
    return false;

  }

  //Breadth First Search
  this.hasPath2 = function(val1,val2){
    //make sure both nodes are in the map
    if(!this.find(val1) || !this.find(val2)){
      console.log('here',val1,val2);
      return false;
    }

    var node = this.map[val1];

    //break case
    if(node['value'] == val2){
      return true;
    }

    for(key in node['connections']){


      if(!(key in this.visited)){
        this.visited[key] = true;
        this.queue.push(key);
      }


    }

    console.log(this.queue);
    if(this.queue.length == 0){
      return false;
    }else{
      //dequeues the first node from the queue so that we can check it
      var nextNodeToCheck = this.queue.shift();
      return this.hasPath(nextNodeToCheck,val2);
    }




  }

  this.shortestPath = function(val1,val2,sum,array){

    if(sum == undefined){

      if(this.hasPath(val1,val2) == false){
        return false;
      }
      this.visited = {};
      sum = 0;
      array = [];
    }

    var node = this.map[val1];

    //break case
    if(node['value'] == val2){
      array.push(sum);
      return;
    }else{
      //make sure node is visited
      this.visited[val1] = true;
    }


    for(key in node['connections']){
      console.log(node['value'],key);
      console.log(this.visited);
      if(!(key in this.visited)){
        this.shortestPath(key, val2, sum + 1, array);
      }

    }

    return smallest(array);
  }


  // private

  function smallest(array){
    var min = array[0];

    for(var i = 1; i < array.length; i++){
      if(min > array[i]){
        min = array[i];
      }
    }

    return min;
  }

}

function Node(value){
  this.value = value;
  this.connections = {};

}



var graph = new Graph();
graph.connect("p","e");
graph.connect("p","c");
graph.connect("c","k");
graph.connect("k","z");
graph.connect("e","z");
graph.connect("p","j");
graph.connect("i","z");


console.log(graph.shortestPath("p","z"));
// console.log(graph.queue)
// console.log(graph['map']['z']['connections']);