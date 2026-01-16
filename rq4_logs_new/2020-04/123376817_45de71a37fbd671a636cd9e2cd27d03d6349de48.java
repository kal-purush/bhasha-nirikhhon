package medium.math;

/**
 * @author: wenhanglei
 * @date: 2020/04/03
 * @time: 15:32
 * @info: Write a program to find the n-th ugly number.
 */
public class UglyNumberII {

  /**
   * 简单朴素版本
   * @param n
   * @return
   */
  public int nthUglyNumber01(int n) {
    long[] uglyArr = new long[1691];
    uglyArr[0] = 1;
    uglyArr[1] = 2;
    uglyArr[2] = 3;
    uglyArr[3] = 4;
    uglyArr[4] = 5;
    if(n < 5) {
      return (int) uglyArr[n-1];
    }
    long current = 5, x2 = 0, x3 = 0, x5 = 0;
    int index = 5;
    int i = index-1;
    while(i > 0) {
      if(i == n) {
        break;
      }
      while(uglyArr[i]*2 > current) {
        i--;
      }
      x2 = uglyArr[i+1]*2;
      while(uglyArr[i]*3 > current) {
        i--;
      }
      x3 = uglyArr[i+1]*3;
      while(uglyArr[i]*5 > current){
        i--;
      }
      x5 = uglyArr[i+1]*5;
      current = Math.min(Math.min(x2, x3), x5);
      uglyArr[index++] = current;
      i = index-1;
    }
    return (int) uglyArr[i-1];
  }

  public static void main(String[] args) {
    int n = 1600; //15
    UglyNumberII sol = new UglyNumberII();
    int i = sol.nthUglyNumber01(n);
    System.out.println(i);
  }

}