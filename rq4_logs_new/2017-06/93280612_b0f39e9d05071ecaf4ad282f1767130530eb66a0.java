import java.util.*;
public class OperationCode {
  public static HashMap<String,Operacao > opCodes = new HashMap<String,Operacao>();
  static void criaMapaPadronizado() { //Cria mapa contendo as opCodes
     String[] line = {"MOV [],","MOV ,[]","MOV","MOV [],[]","ADD","SUB","MUL","DIV","INC","DEC","CMP","JMP","JE","JNE",
                      "JG","JL","JGE","JLE","&","|"};
    String[] opCode = {"00001","00010","00011","00100","00101","00110","00111","01000",
                        "01001","01010","01011","01100","01101","01110","01111","10000","10001",
                        "10010","10011","10100"};
    int i = 0;
    while(i < line.length){
      Operacao op = new Operacao(opCode[i]);
      opCodes.put(line[i], op);
      //System.out.println(line[i] +": "+opCodes.get(line[i]).get);
      i++;
    }
}

public static String recupera(String a){ //Recupera a opCode da String de instrução
    if(a.substring(0,2).equalsIgnoreCase("mov")){
        if(a.charAt(4) == '['){
            if(a.charAt(a.length() - 1) == ']'){
                return opCodes.get("MOV [],[]").getOp();
            }
            return opCodes.get("MOV [],").getOp();
        }
    }
    Operacao op = opCodes.get(comando(a));
    if(op != null)
    return op.getOp();
    return null;
}
private static String comando(String palavra){
    int i = 0;
    String[] temp = palavra.split(" ");
    String comando = temp[0];
    return comando;
}
  public static void main(String [] args){
    criaMapaPadronizado(); 
    Scanner scan = new Scanner(System.in);
    String a = scan.nextLine();
    System.out.println(recupera(a));
  }
}