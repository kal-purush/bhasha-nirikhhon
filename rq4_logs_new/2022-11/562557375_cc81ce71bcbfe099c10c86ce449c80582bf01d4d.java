import java.util.ArrayList;

class Stack<T> {
    public T top;
    private ArrayList<T> stackArr = new ArrayList<T>();
    private int length;

    Stack() {
        this.top = null;
        this.length = 0;
    }

    Stack(T o) {
        this.put(o);
    }

    public void put(T o) {
        this.top = o;
        this.stackArr.add(o);
        this.length = this.stackArr.size();
    }

    public T pick() {
        T val;
        if (this.stackArr.size() > 1) {
            this.stackArr.remove(this.stackArr.size()-1);
            this.top = this.stackArr.get(this.stackArr.size()-1);
            this.length = this.stackArr.size();
            val = this.top;
        }
        else if(this.stackArr.size() == 1){
            this.stackArr.remove(this.stackArr.size()-1);
            this.top = null;
            this.length = this.stackArr.size();
            val = this.top;
        }
        else{
            System.out.println("Stack is empty. ");
            val = null;
        }

        return val;
    }

    public T peek() {
        if (this.stackArr.size() > 0) {
            return this.stackArr.get(this.stackArr.size() - 1);
        }
        else{
            System.out.println("Stack is empty. ");
            return null;
        }
    }

    public int getSize() {
        return this.length;
    }
}

public class stackImpl {
    public static void main(String args[]) {
        Stack<Integer> a = new Stack<Integer>();

        //Test cases
        a.put(1);
        a.put(2);
        a.put(3);
        a.put(4);
        System.out.println(a.peek());
        System.out.println(a.pick());
        System.out.println(a.pick());
        System.out.println(a.pick());
        System.out.println(a.pick());
        System.out.println(a.pick());
        System.out.println(a.peek());
    }
}