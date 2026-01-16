using System;
using System.Linq;

public class Test
{
  public static void Main(){
    string s1 = "god";
    string s2 = "dog";

    if(s1.Length != s2.Length){
      Console.WriteLine("Not permutation.Diff Length.");
      return;
    }

    if(String.IsNullOrEmpty(s1) && String.IsNullOrEmpty(s2)){
      Console.WriteLine("permutation. empty");
      return;
    }



    int[] myarry = Enumerable.Repeat(0,256).ToArray();

    for(int i=0; i<s1.Length; i++){
      int num = Convert.ToInt32(s1[i]);
      myarry[num] = myarry[num]+1;
    }

    for(int i=0; i<s2.Length; i++){
      int num = Convert.ToInt32(s2[i]);
      myarry[num] = myarry[num]-1;
    }

    for(int i=0; i<256; i++){
      if(myarry[i] != 0){
        Console.WriteLine("Not permutation.");
        return;
      }
    }

    Console.WriteLine("permutation.");

  }
}