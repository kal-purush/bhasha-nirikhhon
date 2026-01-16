using ConsoleMenu.Library.Menu;
using ConsoleMenu.Library.Models;

namespace ConsoleMenu.Library.Managers;
public class SelectionManager : ISelectionManager
{
    public int CurrentIndex { get; private set; }

    public ChildrenManager Owner { get; init; }

    public SelectionManager(ChildrenManager owner)
    {
        CurrentIndex = 0;
        Owner = owner;
    }
    public bool Decrement()
    {
        if (MayDecrement(out int newIndex))
        {
            RenderSelection(false);
            CurrentIndex = newIndex;
            RenderSelection(true);
            return true;
        }
        return false;
    }

    public bool Increment()
    {
        if (MayIncrement(out int newIndex))
        {
            RenderSelection(false);
            CurrentIndex = newIndex;
            RenderSelection(true);
            return true;
        }
        return false;
    }
    public IChildItem GetSelectedChild()
        => Owner.HaveChildren() == false ? throw new InvalidOperationException() : Owner.GetChild(CurrentIndex);

    private void RenderSelection(bool isSelected)
    {
        if (Owner.HaveChildren() == false)
        {
            throw new ArgumentNullException("No children present!");
        }
        IMenuItem menuItem = GetSelectedChild().Item;
        menuItem.Content.IsSelected = isSelected;
        menuItem.Render();
    }

    private bool MayDecrement(out int newIndex)
    {
        newIndex = -1;
        if (Owner.IsVisible == false || CurrentIndex <= 0)
        {
            return false;
        }

        int i = CurrentIndex;
        while (i > 0)
        {
            i--;
            if (Owner.GetChild(i).Item.IsVisible)
            {
                newIndex = i;
                return true;
            }
        }
        return false;
    }

    private bool MayIncrement(out int newIndex)
    {
        newIndex = -1;
        int childrenCount = Owner.GetChildren().Count();
        if (Owner.IsVisible == false || CurrentIndex >= childrenCount)
        {
            return false;
        }
        int i = CurrentIndex;
        while (i < childrenCount - 1)
        {
            i++;
            if (Owner.GetChild(i).Item.IsVisible)
            {
                newIndex = i;
                return true;
            }
        }
        return false;
    }
}