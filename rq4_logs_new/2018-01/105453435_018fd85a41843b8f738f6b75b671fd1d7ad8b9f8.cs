static extern class NJCLib
{
    //[JavaRef("Library/printString")]
    //static extern void PrintString(string s);

    [JavaRef("Library/printInt")]
    [DotNetRef("DotNetLib/Library/PrintInt")]
    static extern void PrintInt(int i);

    [JavaRef("Library/printBool")]
    [DotNetRef("DotNetLib/Library/PrintBool")]
    static extern void PrintBool(bool b);

    [JavaRef("Library/printChar")]
    [DotNetRef("DotNetLib/Library/PrintChar")]
    static extern void PrintChar(char c);
}