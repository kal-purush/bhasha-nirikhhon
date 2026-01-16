// ReSharper disable UnusedMember.Global
// ReSharper disable InconsistentNaming

namespace Sandpit.Compiler.Lib;

public static class SystemCalls {
    public static void print(string s) => Console.Write(s);

    public static void printLine(string s) => Console.WriteLine(s);

    public static string input(string s) {
        Console.Write(s);
        return Console.ReadLine() ?? "";
    }
}