package Q18;

/**
 * Java言語 プログラミングレッスン 下
 * p258 問題18-3
 * 
 * List18-9は、ファイルの名前を指定すると、そのファイルに
 * 1000より小さい素数の表を書き出すプログラムですが、まだ作りかけです。
 *   java Prime1 prime.txt
 * のように実行すると、prime.txtの内容はFig18-15のようになる予定です。
 * このプログラムを完成させてください。 
 * 
 * List18-19
 * public class Prime1 {
 *   public static void main (String[] args) {
 *     if (args.length != 1) {
 *       System.out.println("使用法 : java Prime1 作成ファイル");
 *       System.out.println("例 : java Prime1 prime.txt");
 *       System.exit(0);
 *     }
 *    
 *     String filename = args[0];
 *     PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(filename)));
 *     writePrime(writer);
 *     writer.close();
 *   }
 *   
 *   public static void writePrime(PrintWriter writer) {
 *     boolean[] prime = new boolean[MAX_PRIME];
 *     
 *     for (int n = 0; n < MAX_PRIME; n++) {
 *       prime[n] = true;
 *     }
 *     
 *     prime[0] = false;
 *     prime[1] = false;
 *     
 *     for (int n = 0; n < MAX_PRIME; n++)  {
 *       if(prime[n]) {
 *         //ここでnを出力する
 *         
 *         for(int i = 2; i * n < MAX_PRIME; i++) {
 *           prime[i * n] = false;
 *         }
 *       }
 *     }
 *   }
 * }
 * 
 * Fig18-15 (実行結果)
 * 
 * 2
 * 3
 * 5
 * 7
 * ・・・(中略)・・・
 * 977
 * 983
 * 991
 * 997
 * 
 */

import java.io.*;

public class Prime1 {
  static final int MAX_PRIME = 1000;
  
  public static void main (String[] args) {
    if (args.length != 1) {
      System.out.println("使用法 : java Prime1 作成ファイル");
      System.out.println("例 : java Prime1 prime.txt");
      System.exit(0);
    }
    
    try {
      String filename = args[0];
      PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(filename)));
      writePrime(writer);
      writer.close();
    } catch (IOException e) {
      System.out.println(e);
    }
  }
  
  public static void writePrime(PrintWriter writer) {
    boolean[] prime = new boolean[MAX_PRIME];
    
    for (int n = 0; n < MAX_PRIME; n++) {
      prime[n] = true;
    }
    
    prime[0] = false;
    prime[1] = false;
    
    for (int n = 0; n < MAX_PRIME; n++)  {
      if(prime[n]) {
        writer.println(n);
        
        for(int i = 2; i * n < MAX_PRIME; i++) {
          prime[i * n] = false;
        }
      }
    }
  }
}