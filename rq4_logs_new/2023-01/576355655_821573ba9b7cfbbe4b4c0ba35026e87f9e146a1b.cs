using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Drawing;
using System.Drawing.Imaging;
using System.Threading;
using System.Windows.Forms;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Linq;

public class Category
{
    public async Task Cat()
    {
        await Task.Run(() =>
        { // Test to manage files with contents
            string catString = File.ReadAllText("C:/Users/guven/Desktop/categoriesTest.json");
            string txtPath = "C:/Users/guven/Desktop/kmath.txt";
            var jsonObj = JsonConvert.DeserializeObject<dynamic>(catString);
            List<string> categories = new List<string>();

            foreach (var category in jsonObj)
            {
                categories.Add(category); // Adds each category to list
            }
            
            File.WriteAllLines(txtPath, categories); // Writes to text file all categories
        });
    }
}