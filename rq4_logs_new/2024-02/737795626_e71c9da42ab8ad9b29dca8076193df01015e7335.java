import java.util.Scanner;
class Stack{
    private int stackSize;
    private char[] stackArr;
    private int top;

    public Stack(int size){
        stackSize = size;
        stackArr = new char[stackSize];
        top = -1;
    }
    public void push(char item){
        stackArr[++top] = item;
    }
    public char pop(){
        char item = peek();
        stackArr[top--] = '\0';
        return stackArr[top];
    }
    public char peek(){
        return stackArr[top];
    }
    public boolean isStackEmpty(){
        return (top==-1);
    }
    public boolean isStackFull(){
        return (top == stackSize);
    }
}
class DelimiterMatching{
    public boolean isDelimiterMatching(String exp){
        Stack myStack = new Stack(exp.length());

        for(int i=0 ; i < exp.length() ; i++){
            char ch = exp.charAt(i);
            if(ch=='(' || ch == '{' || ch == '['){
                myStack.push(ch);
            }
            else if(ch==')' || ch == '}' || ch == ']'){
               if(myStack.isStackEmpty()){
                   return false;
               }
               else{
                   char stackContent = myStack.pop();
                   if((ch == ')' && stackContent != '(')||(ch == '}' && stackContent != '{')||(ch == ']' && stackContent != '[')){
                       return false;
                   }
               }
            }

        }
        return myStack.isStackEmpty();
    }
}
public class Main {
    public static void main(String[] args) {
        Scanner in = new Scanner(System.in);
        DelimiterMatching D = new DelimiterMatching();
        String a;
        System.out.print("Enter String: ");
        a = in.nextLine();

        if (D.isDelimiterMatching(a)) {
            System.out.println("Not Balanced");
        } else {
            System.out.println("Balanced");
        }
    }

}