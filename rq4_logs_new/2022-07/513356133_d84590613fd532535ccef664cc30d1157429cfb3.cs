/*

            This class is still not completed 
            and it will be an extention on the functionality 
            from the Options class  , but it will basically just read
            only the arrow keys from a keayboard            

*/


using System;
namespace QuickTools
{
      public class Key
      {

            private static int Position = 0;
            // private static int Depth = 0; 
            private static int ArrayLength = 0;
            private static void DisplayArray()
            {

                  int[] a = { 1, 2, 3, 4, 5 };

                  /*
                   * 
                  int[] a = { 0, 1, 2, 3, 4 };
                  int[] b = { 5, 6, 7, 8, 9 };           
                  int[] c = {-1, 0, 1, 2, 3 };
                  int[] d = {-2, -1, 0, 1,2};
                  int[] e = { -3,-2,-1,0,1 };
                  int[] f = { -4,-3, -2,-1,0}; 
                  */
                  ArrayLength = a.Length;
                  C.Write("[");
                  for (int i = 0; i < a.Length; i++)
                  {

                        string comma = i < a.Length - 1 ? "," : "";

                        if (a[i] == Position)
                        {
                              Get.LabelSingle(a[i] + comma);
                        }
                        else
                        {
                              C.Write(a[i] + comma);

                        }


                  }
                  C.Write("]");

            }

            public static int Read()
            {
                  int x = 0;


                  //Get.Alert("Text");

                  /*
                      Escape
                      UpArrow
                      DownArrow
                      RightArrow               
                      LeftArrow
                  */

                  DisplayArray(); // show the array 
                  while (Get.KeyInput().ToString() != "Enter")
                  {
                        switch (Get.Key)
                        {

                              case "LeftArrow":
                                    if (Position == 0)
                                    {
                                          Position = ArrayLength;
                                    }
                                    else
                                    {
                                          Position--;
                                    }
                                    DisplayArray();

                                    break;
                              case "RightArrow":
                                    if (Position == ArrayLength)
                                    {
                                          Position = 0;
                                    }
                                    else
                                    {
                                          Position++;
                                    }
                                    DisplayArray();
                                    break;
                              default:

                                    Get.Title("To Exit Press Escape");
                                    break;
                        }
                  }





                  return x; 
            }
      }
}