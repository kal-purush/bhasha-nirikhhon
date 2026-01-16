using ConsoleMenu.Library.Models;

namespace ConsoleMenu.Library.Render;
public class CheckboxContentRender : ContentRender
{
    private static ConsoleColor _normalFgColor = ConsoleColor.Black;
    private static ConsoleColor _selectedColor = ConsoleColor.Blue;
    private ConsoleColor _foregroundColor = _normalFgColor;
    private ConsoleColor _backgroundColor = ConsoleColor.Gray;

    public override Vector2 AreaNeeded() => new(Content.Length + 4, 1);

    public override void Render(Vector2 position) =>
        Render(position, x =>
        {
            var IsMarkedChar = IsMarked ? "X" : " ";
            _foregroundColor = IsSelected ? _selectedColor : _normalFgColor;

            WriteAtPosition(position, $"[{IsMarkedChar}] {Content}", _foregroundColor, _backgroundColor);
        });
}