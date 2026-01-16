class Node{
  constructor(value){
  this.next = null;
  this.value = value;
  }
}

class LinkedList{
  constructor(initialValue){
   let node = initialValue? new Node(initialValue) : null;
   this.tail = node;
   this.head = node;
  }

  addToTail(value){
    let newTale = new Node(value);
   if (!this.head){
     console.log('here');
     this.tail = newTale;
     this.head = newTale;
   }else{
    this.tail.next = newTale;
    this.tail = newTale;
   }
  }
   removeHead(){
     var temp = this.head;
     if(this.head.next === null){
       this.tail = null;
       this.head = null;
     }else{
     this.head = this.head.next;
     }
     return temp;
   }

   contains(value , start = this.head){
     if(start.value === value){
       return true;
     } else if(start.next === null){
       return false;
     }else{
       return true && this.contains(value , start.next);
     }
   }
}