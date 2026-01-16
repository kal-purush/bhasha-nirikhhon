import java.util.Scanner;
public class dictionary{
  public static viod main(String[] main){
    Scanner read = new Scanner(new FileReader("dictionary"));
    while(read.hasNextLine()){
      if(args[0].equals(read.nexLine())){
        System.out.println("Found");
        break;
      }
    }
    System.out.println("Not found");
  }
}