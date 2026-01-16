import { LinkedList } from '../linked-list';
import { Queue } from '../queue';
import { Stack } from '../stack';

export class Graph {
  public vertices: number;
  public list: LinkedList[];
  constructor(vertices: any) {
    // Total number of vertices in the graph
    this.vertices = vertices;
    this.list = [];
    // Creating a new LinkedList for each vertex/index of the list
    for (let i = 0; i < vertices; i++) {
      const temp = new LinkedList();
      this.list.push(temp);
    }
  }

  public addEdge(source: number, destination: any): void {
    // Since we are implementing a directed list, (0,1) is not the same as (1,0)
    this.list[source].insertAtHead(destination);
    // If we were to implement an undirected graph where (0,1)==(1,0),
    // we would create an additional edge from destination to source too:
    // self.list[destination].insertAtHead(source);
  }

  public printGraph(): void {
    console.log('>>Adjacency List of Directed Graph<<');
    for (let i = 0; i < this.list.length; i++) {
      process.stdout.write('|' + String(i) + '| => ');
      let temp = this.list[i].getHead.nextElement;
      while (temp != null) {
        process.stdout.write('[' + String(temp.data) + '] -> ');
        temp = temp.nextElement;
      }
      console.log('null ');
    }
  }

  public dfsTraversal(source: any): string {
    let result = '';
    const numOfVertices = this.vertices;
    // An array to hold the history of visited nodes
    // Make a node visited whenever you push it into stack
    const visited = [];
    for (let x = 0; x < numOfVertices; x++) {
      visited.push(false);
    }
    // Create Stack(Implemented in previous lesson) for Depth First Traversal and Push source in it
    const stack = new Stack();
    stack.push(source);
    visited[source] = true;
    // Traverse while stack is not empty
    while (!stack.isEmpty()) {
      // Pop a vertex/node from stack and add it to the result
      const currentNode = stack.pop();
      result += String(currentNode);
      /** Get adjacent vertices to the currentNode from the array,
       * and if they are not already visited then push them in the stack
       */
      let temp = this.list[currentNode].getHead.nextElement;
      while (temp != null) {
        if (!visited[temp.data]) {
          stack.push(temp.data);
        }
        temp = temp.nextElement;
      }
      visited[currentNode] = true;
    }
    return result;
  }

  public bfsTraversal(source: any): string {
    let result = '';
    const numOfVertices = this.vertices;
    // Alist to hold the history of visited nodes
    // Make a node visited whenever you enqueue it into queue
    const visited = [];
    for (let i = 0; i < numOfVertices; i++) {
      visited.push(false);
    }
    // Create Queue(implemented in previous lesson) for Breadth First Traversal and enqueue source in it
    const queue = new Queue();
    queue.enqueue(source);
    visited[source] = true;
    // Traverse while queue is not empty
    while (!queue.isEmpty()) {
      // Dequeue a vertex/node from queue and add it to result
      const currentNode = queue.dequeue();
      result += String(currentNode);
      // Get adjacent vertices to the currentNode from the list,
      // and if they are not already visited then enqueue them in the Queue
      let temp = this.list[currentNode].getHead.nextElement;
      while (temp != null) {
        if (!visited[temp.data]) {
          queue.enqueue(temp.data);
          visited[temp.data] = true; // Visit the children
        }
        temp = temp.nextElement;
      }
    }
    return result;
  }

  public detectCycle(source: number): boolean {
    const numOfVertices = this.vertices;
    // An array to hold the history of visited nodes
    // Make a node visited whenever you push it into stack
    const visited = [];
    for (let x = 0; x < numOfVertices; x++) {
      visited.push(false);
    }
    // Create Stack(Implemented in previous lesson) for Depth First Traversal and Push source into the stack
    const stack = new Stack();
    stack.push(source);
    visited[source] = true;
    // Traverse while stack is not empty
    while (!stack.isEmpty()) {
      // Pop a vertex/node from stack
      const currentNode = stack.pop();
      /** Get adjacent vertices to the currentNode from the list,
       *  and if anyone of them is already in visited array then cycle exists so
       *  return true
       */
      let temp = this.list[currentNode].getHead.nextElement;
      while (temp != null) {
        if (!visited[temp.data]) {
          stack.push(temp.data);
          visited[temp.data] = true;
        } else {
          return true;
        }
        temp = temp.nextElement;
      }
    }
    return false;
  }

  public numEdges(): number {
    // For undirected graph, just sum up the size of
    // all the adjacency lists for each vertex
    let sum = 0;
    for (let i = 0; i < this.vertices; i++) {
      let temp = this.list[i].head.nextElement;
      while (temp != null) {
        sum += 1;
        temp = temp.nextElement;
      }
    }

    // Half the total sum as it is an undirected graph
    return sum / 2;
  }

  public checkPath(source: any, destination: any): boolean {
    // An array to hold the history of visited nodes (by default all false)
    // Make a node visited whenever you push it into stack
    const visited = [];
    for (let x = 0; x < this.vertices; x++) {
      visited.push(false);
    }
    // Create Stack
    const stack = new Stack();
    stack.push(source);
    visited[source] = true;
    // Traverse while stack is not empty
    while (!stack.isEmpty()) {
      // Pop a vertex/node from stack
      const currentNode = stack.pop();
      /**
       * Get adjacent vertices to the current_node from the list,
       * and if only push unvisited adjacent vertices into stack
       * Before pushing into stack, check if it's the destination
       */
      let temp = this.list[currentNode].head.nextElement;
      while (temp != null) {
        if (!visited[temp.data]) {
          if (temp.data === destination) {
            return true;
          }
          stack.push(temp.data);
          visited[temp.data] = true;
        }
        temp = temp.nextElement;
      }
    }
    return false;
  }

  // You can check the input graphs in console tab
  public isTree() {
    // All vertices unvisited
    const visited = [];
    for (let j = 0; j < this.vertices; j++) {
      visited[j] = false;
    }

    // Check cycle using recursion stack
    // Also mark nodes visited to check connectivity
    if (this.checkCycle(0, visited, -1)) {
      return false;
    }

    // Check if all nodes we visited from the source (graph is connected)
    for (let i = 0; i < visited.length; i++) {
      // Graph is not connected
      if (!visited[i]) {
        return false;
      }
    }
    // Not cycle and connected graph
    return true;
  }

  public checkCycle(node: any, visited: any, parent: any): boolean {
    // Mark node as visited
    visited[node] = true;

    // Pick adjacent node and run recursive DFS
    let adjacent = this.list[node].head.nextElement;
    while (adjacent) {
      if (
        !visited[adjacent.data] &&
        this.checkCycle(adjacent.data, visited, node)
      ) {
        return true;
      }
      // If adjacent is visited and not the parent node of the current node
      else if (adjacent.data !== parent) {
        // Cycle found
        return true;
      }
      adjacent = adjacent.nextElement;
    }
    return false;
  }

  public removeEdge(source: any, dest: any): Graph {
    if (this.list.length === 0) {
      return this;
    }

    if (source >= this.list.length || source < 0) {
      return this;
    }

    if (dest >= this.list.length || dest < 0) {
      return this;
    }

    this.list[source].deleteVal(dest);
    return this;
  }
}