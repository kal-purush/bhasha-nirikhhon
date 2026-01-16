

//1.write a program which checks wheather 15th bit is ON or OFF
import java.util.*;

//typedef unsigned int UINT
class Program1
{  
    public static boolean CheckBit(int iNo)
    {
         int iMask = 0X00004000;
         int iResult =0;

         iResult = iNo & iMask;

         if(iResult == iMask)
         {
             return true;
         }
         else
         {
             return false;

         }

    }
    public static void main(String Args[])
    {
       Scanner sobj = new Scanner(System.in);
       
       int iNo = 0;
       boolean bret = false;

       System.out.println("Enter number :");
       iNo = sobj.nextInt();
       
        bret = CheckBit(iNo);
       if(bret == true)
       {
         System.out.println("15th bit is ON");

       }
       else
       {
           System.out.println("15th bit is OFF");
       }
      
    }
}
