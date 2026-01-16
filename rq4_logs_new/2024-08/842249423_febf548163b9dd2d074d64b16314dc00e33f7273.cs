using TJC.MVVM.Models;

namespace TJC.MVVM.ViewModels;

public abstract class ViewModelBase<T>
    : ViewModelBase
    where T : ModelBase
{
    protected ViewModelBase(T model)
    {
        Model = model;
        Refresh();
        Model.RefreshEvent += delegate { DoRefresh(); };
    }

    public T Model { get; }
}

public abstract class ViewModelBase
{
    protected ViewModelBase()
    {
        Refresh();
    }

    public void Refresh()
    {
        DoRefresh();
    }

    protected abstract void DoRefresh();
}