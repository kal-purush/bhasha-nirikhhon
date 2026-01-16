using ConsoleMenu.Library.Menu;
using ConsoleMenu.Library.Models;

namespace ConsoleMenu.Library.Render;
internal class Content : IContent
{
    private Action<IMenuItem> _action;
    private Func<IMenuItem, Vector2> _areaNeeded;

    public bool IsSelected { get; set; }
    public bool IsMarked { get; set; }
    public required string Title { get; set; }
    public required IMenuItem Owner { get; set; }

    public Content()
    {
        IContentRenderer contentRenderer = new DefaultContentRender();
        SetRenderer(contentRenderer.Render, contentRenderer.AreaNeeded);
    }

    public void SetRenderer(Action<IMenuItem> action, Func<IMenuItem, Vector2> areaNeeded)
    {
        _action = action;
        _areaNeeded = areaNeeded;
    }

    public Vector2 AreaNeeded() => _areaNeeded?.Invoke(Owner);
    public void Render()
    {
        ConsoleColor tempBgColor = Console.BackgroundColor;
        ConsoleColor tempFgColor = Console.ForegroundColor;

        _action?.Invoke(Owner);

        Console.BackgroundColor = tempBgColor;
        Console.ForegroundColor = tempFgColor;
    }
}