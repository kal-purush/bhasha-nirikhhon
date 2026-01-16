public class MyHeap{
  public static void heapify(int[] ary){}

  private static void pushUp(int[] ary, int size, int index){
    if(index * 2 + 1 > size){
      return;}
    if(index * 2 + 2 > size){
      if(ary[index] < ary[index * 2 + 1]){
        swap(ary, index, index * 2 + 1);}
      return;}
    if(ary[index] < ary[index * 2 + 1] && ary[index] < ary[index * 2 + 2]){
      swap(ary, index, max(ary[index * 2 + 1], ary[index * 2 + 2]));
      index = index * 2 + 1;}
    pushUp(ary, size, index);}

  private static void swap(int[] ary, int ind1, int ind2){
    int temp = ary[ind2];
    ary[ind2] = ary[ind1];
    ary[ind1] = temp;}

  private static int max(int a, int b){
    if(a < b){
      return b;}
    return a;}




}