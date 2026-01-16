using ConsoleMenu.Library.Menu;
using ConsoleMenu.Library.Managers;

namespace ConsoleMenu.Library.PerformAction;
public class ActionToPerform
{
    public static bool MoveSelection(ConsoleKeyInfo key, IMenuItem menuItem)
    {
        switch (key.Key, menuItem.Children.ContentOrientation)
        {
            case (ConsoleKey.UpArrow, ContentOrientation.Vetical):
                menuItem.Children.DecrementSelection();
                return false;
            case (ConsoleKey.DownArrow, ContentOrientation.Vetical):
                menuItem.Children.IncrementSelection();
                return false;
            case (ConsoleKey.RightArrow, ContentOrientation.Horizontal):
                menuItem.Children.IncrementSelection();
                return false;
            case (ConsoleKey.LeftArrow, ContentOrientation.Horizontal):
                menuItem.Children.DecrementSelection();
                return false;
        }
        return true;
    }
}