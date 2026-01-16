// See https://aka.ms/new-console-template for more information
//Translate.exe resxfile language deeplapikey

using System.Xml;
Console.Title = "Blogifier Translator";
if (args.Length > 0)
{
    Console.WriteLine(args[0]);
    StartMenu(args[0]);
}
else
{
    SmoothConsoleColor("Argument Error", ConsoleColor.Red);
    Console.WriteLine(@"Please enter the Path to the Resource File, without "" at the start and end:");
    StartMenu(Console.ReadLine());
}


void TranslateManual(string sourcefile)
{
    //Load XML
    XmlDocument doc = new XmlDocument();
    try
    {
        doc.Load(sourcefile);

        int alreadyTranslated = 0;
        //List Nodes to translate
        XmlNodeList textNodes = doc.SelectNodes("/root/data/value");
        Console.WriteLine("Translating: " + textNodes.Count + "entries");
        //Loop through all nodes
        foreach (XmlNode textNode in textNodes)
        {
            //Prompt to Translate

            textNode.InnerText = TranslateTextManual(textNode.InnerText);

            int x = Console.CursorLeft;
            int y = Console.CursorTop;
            alreadyTranslated = alreadyTranslated + 1;
            string alreadyTranslatedText = alreadyTranslated + "/" + textNodes.Count + " Translated";
            Console.SetCursorPosition(Console.WindowWidth - alreadyTranslatedText.Length, Console.WindowHeight);
            Console.WriteLine(alreadyTranslatedText);
            Console.SetCursorPosition(x, y);
        }

        //Save XML
        doc.Save(sourcefile);


        Console.WriteLine("Translation Finished");
        Console.ReadKey();
    }
    catch(Exception e)
    {
        Console.WriteLine(e.Message);
    }
}

string TranslateTextManual(string input)
{
    string output;
    Console.Write(input + ":");
    output = Console.ReadLine();


    return output;
}

void TranslateAuto(string sourcefile)
{
    Console.WriteLine("COMINGSOON");
}

string TranslateTextAuto(string input)
{
    return "";
}

void StartMenu(string path)
{
    Console.WriteLine("[1] Translate Manual");
    Console.WriteLine("[2] Translate Auto");
    Console.WriteLine("[3] Help");
    switch (Console.ReadLine())
    {
        case "1":
            Console.WriteLine("Starting manual Translation");
            TranslateManual(path);
            break;
        case "2":
            Console.WriteLine("Starting auto Translation");
            TranslateAuto(path);
            break;
        case "3":
            SmoothConsole("Blogifier.Translator Help");
            SmoothConsole("Manual Translation: You get prompted to enter the English Word in the language you want to translate to.");
            SmoothConsole("Auto Translation: COMING SOON");
            SmoothConsole("Press Any Key to continue.");
            Console.ReadKey();
            Console.Clear();
            StartMenu(path);
            break;
    }
}

void SmoothConsole(string text)
{
    foreach (char character in text + "\n")
    {
        Thread.Sleep(25);
        Console.Write(character);
    }
}

void SmoothConsoleColor(string text, ConsoleColor color)
{
    Console.ForegroundColor = color;
    foreach (char character in text + "\n")
    {
        Thread.Sleep(25);
        Console.Write(character);
    }
    Console.ResetColor();
}