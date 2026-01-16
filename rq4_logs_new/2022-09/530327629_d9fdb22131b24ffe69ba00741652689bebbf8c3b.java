package Questions;

public class CommandInterpreter {
     public static void main(String[] args) {
        System.out.println(interpret("G()(al)"));
     }
     public static String interpret(String command) {
 StringBuilder sb=new StringBuilder();
 for (int i = 0; i < command.length(); i++) {
    if(command.charAt(i)=='G'){
        sb.append("G");
    }
    if(command.charAt(i)=='('){
      if (command.charAt(i+1)==')') {
     sb.append("O") ; 
       }else{
        sb.append("al");
       }
    }
 }
 return sb.toString();
    }

}