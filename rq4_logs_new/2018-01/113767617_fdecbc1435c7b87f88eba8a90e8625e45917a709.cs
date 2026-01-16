using UnityEngine;

public abstract class ItemSlot : UISlot {
    public bool IsEmpty { get { return Content == null; } }
    protected Collectable Content;

    private void OnGUI()
    {
        var e = Event.current;
        if(e.type == EventType.MouseUp && e.button == 1)
        {
            OnRightClick();
        }
    }

    internal void Add(Collectable collectable)
    {
        Content = collectable;
        SetIcon(collectable.Icon);
    }

    protected void EmptySlot()
    {
        Content = null;
        SetIcon(null);
    }

    protected abstract void OnRightClick();
}