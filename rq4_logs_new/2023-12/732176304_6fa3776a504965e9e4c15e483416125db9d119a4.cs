using ConsoleMenu.Library.Models;
using ConsoleMenu.Library.RenderComposites;

namespace ConsoleMenu.Library.Menu;
internal class MenuItem : IMenuItem
{
    private readonly List<RenderComposite> _renderComposites;
    public Vector2 Position { get; set; }

    public MenuItem()
    {
        _renderComposites = new();
    }

    public void AddRenderComposite(int priority, IRenderComposite render)
    {
        var composite = new RenderComposite(this, render, priority);
        _renderComposites.Add(composite);
    }

    public void RemoveRenderComposite(IRenderComposite render)
    {
        var composite = _renderComposites.Where(x => x.Render == render).FirstOrDefault();
        if (composite is null)
            throw new ArgumentNullException(nameof(composite));
        _renderComposites.Remove(composite);
    }

    public IEnumerable<RenderComposite> GetRenderComposites() 
        => _renderComposites.OrderBy(x => x.RenderPriority);

    public Vector2 AreaNeeded() => throw new NotImplementedException();
    public void Render() => throw new NotImplementedException();
}