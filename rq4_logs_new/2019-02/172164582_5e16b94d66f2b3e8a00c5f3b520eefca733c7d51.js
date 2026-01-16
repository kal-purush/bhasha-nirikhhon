export class Algorithm{
        //all coordinates are in array with x and y coordinate
        constructor(start, end) {
            this.startNode = start;
            this.endNode = end;
            this.currentNode = start; 
            this.openList = [this.startNode]; //array of objects (node)
            this.closeList = []; //array of objects (node)
            this.selectedPath = [this.startNode]; //array of objects (node)

        }

        calculateGcosts() {
            let totalDistance = 0                       
            for (let r in this.selectedPath) {                     
             //calculate distance between each node in selected path except first node in array            
             if ((r != 0) && (this.selectedPath.length != 1)) {                
                totalDistance += Math.sqrt(Math.pow((this.selectedPath[r].x - this.selectedPath[r - 1].x), 2) + Math.pow((this.selectedPath[r].y - this.selectedPath[r - 1].y), 2))                
             }                        
            } 

            return totalDistance
        }

        calculateCosts(current, end){
            //If selected path array length is 0, calculate distance between current node and starting node             
            let selectedDist = this.calculateGcosts()  
            let g = selectedDist + Math.sqrt(Math.pow((current.x - this.selectedPath[this.selectedPath.length - 1].x), 2) + Math.pow((current.y - this.selectedPath[this.selectedPath.length - 1].y), 2))            
            //calculate distance between current node and the last node in selected path array 
            //and combine the distance between all nodes within selected node             
            let h = Math.sqrt(Math.pow((end.x - current.x), 2) + Math.pow((end.y - current.y), 2));
            let f  = 0.2*g + 0.8*h
            return {f, h}
        }

        //for nodes that are unwalkable or already been walked
        addCloseList(...closeNode) {
            closeNode.forEach(node => {
                this.closeList.push(node)    
            })            
        }        

        findChildrenNodes(coordinate) {
            let node = coordinate;
            let childrens = [];
            let sequence = [-1, 0, 1]
            for (let i in sequence) {
                let nodeDuplicateX = {...node};
                nodeDuplicateX.x += sequence[i]
                for (let r in sequence) {
                    let nodeDuplicateY = {...nodeDuplicateX}
                    nodeDuplicateY.y += sequence[r]
                    childrens.push(nodeDuplicateY)
                }
            }

             // if childrens appear in open list already, ignore it 
             let openList = [...this.openList.slice()]
             let openListCopy = [...openList]                   
             childrens.forEach(children => {
                 let duplicate = []
                 openList.forEach(openNode => {
                     if (((children.x === openNode.x) && (children.y === openNode.y))) {
                         duplicate.push(openNode)}
                 })        

                 if (duplicate.length === 0) {openListCopy.push({...children})                
                }    
                 duplicate = []; 
             })
             this.openList = openListCopy.slice()
             
             return childrens             
            
        }

        findGoal(){
            this.addCloseList({x:5, y:9}, {x:6, y:9}, {x:7, y:9})            
            while ((this.currentNode.x !== this.endNode.x) || (this.currentNode.y !== this.endNode.y)) {            
                //find children nodes
            this.findChildrenNodes(this.currentNode)            
            let i;
            let removedOpenList = [];

            //Compare close array and open array, if match, then add open node index to removedOpenList array
            for (i = 0; i < this.openList.length; i++) {
                let r;                                
                for (r = 0; r < this.closeList.length; r++) {                    
                    if ((this.openList[i].x === this.closeList[r].x) && (this.openList[i].y === this.closeList[r].y)) {                        
                        removedOpenList.push(i)
                    }
                }                
            }
            //filter open array node out 
            let updatedOpenList = this.openList.filter(node => !removedOpenList.includes(this.openList.indexOf(node)) )
            this.openList = [...updatedOpenList]            
                        
            let currentIndex = this.openList.findIndex(node => {
                return (node.x === this.currentNode.x && node.y === this.currentNode.y)
            })

            //Remove current node from open list
            this.openList.splice(currentIndex, 1);
                       
            //calculate costs for each children nodes
            let openList = [...this.openList.slice(0)]

            //create costs list with f and h costs
            let costsList = openList.map(node => 
                this.calculateCosts(node, this.endNode)
            )

            //create f costs with seperate array costsF and create node array with minimum f cost
            let costsF = costsList.map(f => {
                return f.f
            })         

            //find the minimum f and h cost among lists of costs
            let minF = Math.min(...costsF);

            //select index with lowest f cost
            let f;
            let minFcostsIndex = []            
            for (f = 0; f < costsList.length; f++) {
                if (costsList[f].f === minF) {
                    minFcostsIndex.push(f)
                }
            }

            //create h cost array with nodes that have minimum f costs and select minimum h cost
            let hCost = [];
            minFcostsIndex.forEach(index => {
                let h = costsList[index].h;
                hCost.push({index, h})
            })


            let h;
            let index; 
            let minH;
            for (h = 0; h < hCost.length; h++) {      
                minH = hCost[0].h                          
                if (hCost.length === 1) {                                       
                    index = hCost[h].index
                }
                if (hCost[h].h <= minH) {
                    minH = hCost[h].h
                    index = hCost[h].index
                }
            }

            let lowestCostNode = Object.assign({}, openList[index]);
            // Push node with lowest cost into selected path
            this.selectedPath.push(lowestCostNode)
            //Push current node into clost list 
            this.closeList.push(this.currentNode);
            //Remove current node from open list
            this.openList.splice(currentIndex, 1);
            // change current node into node with lowest cost
            this.currentNode = lowestCostNode;
            console.log(this.currentNode)
            console.log("selected Path", this.selectedPath)
            
         }
        }        
        
    }






