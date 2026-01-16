package q1;

// クラス名は大文字から始める
public class Q1 {
  
  public static void main(String[] args) {
    //可読性のため、変数は無駄に使わない。代入回数も極力減らす。
    int sum = 0;
    
    //定数にはfinalをつけて変更不可にする。また定数であることをわかりやすくするため、全て大文字で表記する。
    //この問題の場合、和を求める自然数の上限を定数として外に出す。LIMITの値を10に書き換えれば、問題文のケースを簡単にテストできる。
    final int LIMIT = 1000;

    // 無限ループに陥るバグの可能性を極力減らすため、ループには必ずbreak以外の終了条件をつける。
    for (int i = 1; i < LIMIT; i++) {
      //条件は簡潔に書く。
      //iを3またはで割った余りが0の場合、iをsumに加算する
      if (i % 3 == 0 || i % 5 == 0) {
        sum = sum + i;
      }
    }
    
    System.out.println(sum);
  }
}