using System;
using System.Collections;
using System.IO;

public class SearchDirectory
{
   public static void Main()
   {
      string [] fs = Directory.GetFiles(@"/Users/codeforcoffee/github/security_pract");
      bool foundRuby = false;
      int i = 0;
      for (i = 0; i < fs.Length; i++)
         if (String.Compare(Path.GetExtension(fs[i]), ".rb", true) == 0)
         {
            foundRuby = true;
            break;
         }
   
     if (foundRuby)
        Console.WriteLine("Ruby file found - " + fs[i]);
     else
        Console.WriteLine("No Ruby files found.");
      
   }
}