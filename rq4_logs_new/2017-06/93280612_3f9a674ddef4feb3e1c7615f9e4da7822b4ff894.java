public class OperationCode {
   static HashMap<String,Operacao > opCodes = new HashMap<String,Operacao>();
  static void execute(String [] line , String [] opCode) {
      int i = 0;
      while(i < line.length){
       Operacao op = new Operacao(opCode[i]);
       opCodes.put(line[i], op);
      System.out.println(line[i] +": "+opCodes.get(line[i]).opCode);
      i++;
    }
}
    public static void main(String [] args){
        String[] teste = {"MOV","ADD","SUB","MUL","DIV","INC","DEC","CMP","JMP","JE","JNE",
"JG","JL","JGE","JLE","&","|"};
        String[] opCodes = {"00001","00010","00011","00100","00101","00110","00111","01000",
        "01001","01010","01011","01100","01101","01110","01111","10000","10001"};
        execute(teste , opCodes); 
    }
}