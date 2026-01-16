//Implementing a Stack data structure using linked list. This data structure uses linked list with each node acting as a element in stack.
//Following methods are added to this class
//pop(): removes the last element added to the list
//push(): add an element to the list
//peek(): returns the last element addes to the list
//isEmpty(): returns true if the list is empty.

class MyStack<T>{

    private static class MyStackNode<T>{
        private T data;
        private MyStackNode<T> next;
        public MyStackNode(T data){
            this.data = data;
        }
    }
    private MyStackNode<T> top;

    public T pop(){
        if(top==null) throw new EmptyStackException();
        T value = top.data;
        top = top.next;
        return value;
    }
    public void push(T data){
        MyStackNode<T> node = new MyStackNode<T>(data);
        node.next = top;
        top = node;
    }
    T peek(){
        if(top==null) throw new EmptyStackException();
        return top.data;
    }
    boolean isEmpty(){
        if(top==null) return true;
    }
}