using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace CSharpTricks
{
    class LambdaExpressions
    {
        public static void LambdaExpressionsTutorial()
        {
            Console.WriteLine("LambdaExpressions.LambdaExpressionsTutorial();");
            Console.WriteLine("====== Start ======");

            // Unconscious Assumption - you will be in /bin/Debug or Retail when running locally and the folder is located two levels up
            // Get me all the filepaths from the Resources directory two levels up except files ending in .weird or .WEirD etc.
            IEnumerable<string> filePaths =
                Directory.EnumerateFiles("../../Resources")
                    .Where(f => !Path.GetExtension(f).Equals(".weird", StringComparison.OrdinalIgnoreCase));

            // For each of the files found above, make one entry in this dictionary with the file's name as key and its content as value
            Dictionary<string, byte[]> fileNamesAndBytes = filePaths.ToDictionary(f => f, File.ReadAllBytes);

            // Let me see.
            foreach (KeyValuePair<string, byte[]> entry in fileNamesAndBytes)
            {
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine(entry.Key);
                Console.ForegroundColor = ConsoleColor.DarkGreen;
                // Unconscious Assumption - you're trying to extract strings from bytes - you know all the files you got are txt even if you didn't put this restriction
                Console.WriteLine(System.Text.Encoding.Default.GetString(entry.Value)); // get the file's content as a string from the bytes[]
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("=^.^=");
            }

            Console.ResetColor();
            
            Console.WriteLine("====== End ======");
            Console.WriteLine("LambdaExpressions.LambdaExpressionsTutorial();");

            // write a test for all your unconscious assumptions
            // test to see if the Resources folder is on the same level as this class (good for when you wanna refactor and move this class into its own namespace and folder)
            // test to see if the files you seek are at that path (good for when you wanna refactor)
        }
    }
}