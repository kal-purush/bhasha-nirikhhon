class Stack{
    constructor(){
        this.arr = [];
        this.size = 0;
    }
    // 获取栈顶
    peek(){
        if(this.empty()) return ;
        return this.arr[this.size - 1];
    }
    // push数据
    push(value){
        this.size++;
        this.arr.push(value);
    }
    // pop数据
    pop(){
        if(this.empty()) return ;
        this.size--;
        return this.arr.pop();
    }
    // 清空栈
    clear(){
        if(this.empty()) return ;
        this.size = 0;
        this.arr = [];
    }
    // 判断是否为空
    empty(){
        return this.size == 0 ;
    }
}

var stack = new Stack();
stack.push(1);
stack.push(2);
stack.push(3);
stack.push(4);
console.log(stack)