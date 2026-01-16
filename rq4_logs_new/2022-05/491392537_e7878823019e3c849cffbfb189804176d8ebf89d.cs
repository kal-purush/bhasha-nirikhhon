// See https://aka.ms/new-console-template for more information



Console.WriteLine("('_')?");

string textVariable = "テキスト";
Console.WriteLine(textVariable);
textVariable = "テキスト2";
Console.WriteLine(textVariable);

string textVariable2 = "sdgf";

string conbinetext = textVariable + textVariable2;
Console.WriteLine(conbinetext);

textVariable2 = "gdfgf";
conbinetext = textVariable + textVariable2;
Console.WriteLine(conbinetext);

textVariable2 = " bbb ";
textVariable = "  aaaa ";
conbinetext = textVariable + textVariable2;
Console.WriteLine(conbinetext);

Console.WriteLine("一つ目の文字を入力してください。");
string userinput = Console.ReadLine()??"";
//Console.WriteLine(userinput);
Console.WriteLine("二つ目の文字を入力してください。");
string userinput2 = Console.ReadLine()??"";
Console.WriteLine("入力された文字は" + userinput + "と"+userinput2);
Console.WriteLine(userinput + "と" + userinput2+"の間に入れる文字を入力してください。");
string userinput3 = Console.ReadLine() ??"" ;
Console.WriteLine("入力された文字は　"+　userinput　 +　userinput3 　+ userinput2　+"　です。");
