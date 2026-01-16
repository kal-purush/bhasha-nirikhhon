using ConsoleMenu.Library.Menu;

namespace ConsoleMenu.Program.Playground;
internal class Evolution
{
    public static void Run()
    {

        IMenuItem menuSettings = new MenuItem("Settings");
        menuSettings.Children.Add(1, new MenuItem("Sub 1"));
        menuSettings["Sub 1"].Content.Title = "New Sub 1";
        menuSettings["New Sub 1"].Children.Add(1, new MenuItem("Sub Sub 1"));
        menuSettings[0][0].Content.Title = "New Sub Sub 1";

        menuSettings.Render();


        ConsoleKeyInfo keyInput;
        do
        {
            keyInput = Console.ReadKey(true);
            _ = menuSettings.KeyPressed(keyInput);
        } while (keyInput.Key != ConsoleKey.Escape);

        Console.WriteLine("\n\n\n\n\n\n\n\n\n\n\n\n\n");
    }

}