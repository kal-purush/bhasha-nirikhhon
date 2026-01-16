import java.util.*;
public class Principal {
  static int wordSize = 36; // Tamanho da palavra
  static boolean[] ax = new boolean[wordSize]; 
  static boolean[] bx = new boolean[wordSize]; 
  static boolean[] cx = new boolean[wordSize]; 
  static boolean[] dx = new boolean[wordSize]; 
  static boolean cf, zf, sf, of;
  static HashMap<Boolean[], String> memory = new HashMap<Boolean[], String>();
  boolean[] pc = new boolean[wordSize];
  Stack<String> stack = new Stack<>();

  public static void main(String[] args) {
    // PC Settado pra primeira linha do codigo na memoria
    while(true) {
      String instruction = memory.get(pc);
      Operation.execute(instruction);
      // pc.parseInt
    }
  }

  public static boolean[] somaArrays(boolean[] a, boolean[] b) {
    for(int i = b.length-1; i>=0; i--) {
      boolean carry = false;
      if (b[i] == true && a[i] == true) {
        carry = true;
        b[i] = false;
        
      }
      if (b[i] != a[i]) {
        carry = false;
        b[i] = true;
      }
      if (b[i] == false && a[i] == false) {
        carry = false;
        b[i] = false;
      }
    }
  }
}