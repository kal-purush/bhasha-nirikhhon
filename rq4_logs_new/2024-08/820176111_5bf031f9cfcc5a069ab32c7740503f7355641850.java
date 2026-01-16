package LAB_11;

import java.util.Scanner;

class StackImplement {
    int size;

    public StackImplement(int size){
        this.size=size;
    }

    class Node {

        int info;
        Node link;

        public Node(int info) {

            this.info = info;
            this.link = null;
        }
    }

    public Node top = null;

    public void push(int data){
        if(countNode()>=size){
            System.out.println("Stack Overflow");
            return;
        }
        Node newNode = new Node(data);

        Node temp = top;
        top = newNode;
        top.link = temp;
    }

    public int pop(){
        if(countNode()==0){
            System.out.println("Stack UnderFlow");
            return -1;
        }
        int temp=top.info;
        top=top.link;
        return temp;
    }
    public int peep(int index){
        if(countNode()-index+1<=0){
            System.out.println("Stack Underflow");
            return -1;
        }
        Node save=top;
        for (int j = 0; j < index-1; j++) {
            save=save.link;
        }
        return save.info;
    }
    public void change(int data,int index){
        if(countNode()-index+1<=0){
            System.out.println("Stack Underflow");
            return;
        }
        Node save=top;
        for (int j = 0; j < index-1; j++) {
            save=save.link;
        }
        save.info=data;
    }
    public int countNode() {
        int count = 0;
        if (top == null) {
            count = 0;
            return count;
        }
        Node save = top;
        while (save != null) {
            count++;
            save = save.link;
        }
        return count;
    }
    public void displayStack(){
        if(top==null){
            System.out.println("Stack Is Empty");
            return;
        }
        Node save=top;
        while (save!=null) {
            System.out.println(save.info);
            save=save.link;
        }
    }
}
public class Lab_11_60{
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        System.out.print("Enter Size Of Stack : ");
        int n = sc.nextInt();
        StackImplement stk=new StackImplement(n);
        int m;
        System.out.println("Enter Following Digits For Operations\n");
        System.out.println("1 for push()");
        System.out.println("2 for pop()");
        System.out.println("3 for peep()");
        System.out.println("4 for change()");
        System.out.println("5 for display()");
        System.out.println("6 for exit");

        while (true) {
            System.out.print("Enter Digit for Operation : ");
            m = sc.nextInt();
            switch (m) {
                case 1:
                    System.out.print("Enter Element To Push : ");
                    int x = sc.nextInt();
                    stk.push(x);
                    break;
                case 2:
                    System.out.println(stk.pop());
                    break;
                case 3:
                    System.out.print("Enter Index To Show element : ");
                    int i = sc.nextInt();
                    System.out.println(stk.peep(i));
                    break;
                case 4:
                    System.out.print("Enter Element To Change : ");
                    int y = sc.nextInt();
                    System.out.print("Enter Index To Change : ");
                    int j = sc.nextInt();
                    stk.change(y, j);
                    break;
                case 5:
                stk.displayStack();
                    break;
                case 6:
                System.exit(0);
                    break;

                default:
                    System.out.println("Enter Valid Input");
                    break;
            }
        }
    }
}