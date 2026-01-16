using ConsoleMenu.Library.Menu;
using ConsoleMenu.Library.Models;

namespace ConsoleMenu.Program.Examples;
internal class AddChildren
{
    public static void Run()
    {
        IMenuItem subMenu = new MenuItem("My SubMenu #1");
        subMenu.Children.Add(1, new MenuItem("Sub1"));
        subMenu.Children.Add(2, new MenuItem("Sub2"));
        subMenu.Children.ContentOrientation = Library.Managers.ContentOrientation.Horizontal;

        IMenuItem subMenu2 = new MenuItem("My SubMenu #2");
        subMenu2.Children.Add(1, new MenuItem("Sub1"));
        subMenu2.Children.Add(2, new MenuItem("Sub2"));

        IMenuItem menu = new MenuItem("Simple menu")
        {
            Position = new Vector2(0, 1)
        };
        menu.Children.Add(1, subMenu);
        menu.Children.Add(2, subMenu2);
        menu.Children.Add(3, new MenuItem("Menu 3"));
        menu.Children.ContentOrientation = Library.Managers.ContentOrientation.Horizontal;
        menu.Render();

        // Render() use these to render
        //menu.ContentRenderer.Render(menu.Position);
        //menu.Children.Render();
    }
}