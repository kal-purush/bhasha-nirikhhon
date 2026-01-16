interface Queue {
    push(data:String) : string;
}

type Node = {
    data : string ;
    nextData? : Node ;
}
  
class Queue{
    
    private head : Node;
    private tail : Node;
    private _size : number = 0;
    
    get size(){
        return this._size;
    }

    push(data:string){
        
        let node: Node = {data};
  
        if(!this.head){
            this.head = node ;
            this.head.nextData = this.tail;
        }else{
            this.tail.nextData = node;
        }
  
        this.tail = node;
        this._size++ ;
  
        return data
    }
  
    pop(){
        if(this.head){
            if(this.size <= 1){
                // 큐에 데이터가 1개 이하인 경우 -> head = tail
                let returnData = this.head;
                this.head = null;
                this.tail = null;
                this._size = 0;
                return returnData.data
            }else{
                // 큐에 데이터가 2개 이상 있는 경우
                let returnData = this.head;
                this.head = this.head.nextData;
                this._size += -1;
                return returnData.data
            }  
        }else{
            // 큐에 데이터가 없는 경우
            return -1
        }
    }
  
  }
  
export {Node ,Queue} 
  
  
  