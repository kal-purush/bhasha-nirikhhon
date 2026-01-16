using ConsoleMenu.Library.Models;

namespace ConsoleMenu.Library.Render.Contents;
public class BasicContentRender : IContentRender
{
    private readonly string _title;
    private readonly ConsoleColor _normalColor = ConsoleColor.Black;
    private readonly ConsoleColor _markedColor = ConsoleColor.Green;
    private readonly ConsoleColor _selectedColor = ConsoleColor.Blue;

    public bool IsSelected { get; set; }
    public bool IsMarked { get; set; }

    public BasicContentRender(string title)
    {
        _title = title;
    }
    public Vector2 AreaNeeded() => new(_title.Length + 2, 1);
    public void Render(Vector2 position)
    {
        var tempBgColor = Console.BackgroundColor;
        var tempFgColor = Console.ForegroundColor;

        if (IsMarked)
        {
            Console.ForegroundColor = _markedColor;
            Console.BackgroundColor = _normalColor;
        }
        else
        {
            Console.ForegroundColor = _normalColor;
            Console.BackgroundColor = ConsoleColor.Gray;
        }

        if (IsSelected)
        {
            Console.ForegroundColor = _selectedColor;
            Console.SetCursorPosition(position.X, position.Y);
            Console.Write("[");
            Console.ForegroundColor = IsMarked ? _markedColor : _normalColor;
        }

        Console.SetCursorPosition(position.X + 1, position.Y);
        Console.Write(_title);

        if (IsSelected)
        {
            Console.ForegroundColor = _selectedColor;
            Console.SetCursorPosition(position.X + 1 + _title.Length, position.Y);
            Console.Write("]");
        }

        Console.BackgroundColor = tempBgColor;
        Console.ForegroundColor = tempFgColor;
    }
}