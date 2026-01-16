using Microsoft.Office.Interop.Word;
using System;

namespace WordIntropApp
{
    class Program
    {
        static void Main(string[] args)
        {
            
            Console.WriteLine($"Test Word Introp Object:{SetWordObject()}");
            Console.ReadLine();
        }

        private static string SetWordObject()
        {
            var sourceFile = "C:\\Dev\\TestTemplate.dotx";

            Application oWord = new Application();
            oWord.DisplayAlerts = WdAlertLevel.wdAlertsNone;
            oWord.Visible = false;
            Document oWordDoc = oWord.Documents.Add(sourceFile);
            var controls = oWordDoc.Content.ContentControls;
            foreach (ContentControl item in controls)
            {
                Console.WriteLine(item.PlaceholderText.Value);
            }
            return oWordDoc.FullName;
        }
    }
}