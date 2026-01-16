

using System.Linq;
using System.Reflection.PortableExecutable;

// Taking a string
string str = "(The (quick (brown) fox)) ((jumps) over)(the lazy dog)";

String[] spearator = {")) ((", "(", " (", ")", "))"};

// Split string
String[] strlist = str.Split(spearator,StringSplitOptions.RemoveEmptyEntries);

// Combine string again
string combinedString = string.Join(" ", strlist);

// Remove unnecessary space
var finalString = combinedString.Replace("  ", " ");


// Combine and Output final list
string finalList = finalString[0..20] +"\n" +
                   finalString[4..20] + "\n" +
                   finalString[10..16] + "\n" +
                   finalString[20..26] + "\n" +
                   finalString[20..31] + "\n" +
                   finalString[31..];

Console.WriteLine(finalList);