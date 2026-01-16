using ConsoleMenu.Library.Models.EventArg;

namespace ConsoleMenu.Library.Managers;
internal class SelectionManager : ISelectionManager
{
    private readonly HashSet<int> _selectedItems;

    private int _selectionMin;
    private int _selectionMax;
    public event Action<SelectionChangedEventArgs> SelectionChanged;
    public SelectionManager()
    {
        _selectedItems = new HashSet<int>();
    }

    public IEnumerable<int> SelectedItems() => _selectedItems;
    public void SetSelectionMinMax(int min, int max)
    {
        if (min < 0)
            throw new ArgumentOutOfRangeException(nameof(min), "May not be less than 0");
        if (max <= min)
            throw new ArgumentOutOfRangeException(nameof(max), "May not be less or equal to min");
        _selectionMin = min;
        _selectionMax = max;
    }
    public bool Add(int index)
    {
        if (_selectedItems.Count < _selectionMax)
        {
            if (_selectedItems.Add(index))
            {
                SelectionChanged?.Invoke(new SelectionChangedEventArgs() { Sender = this });
                return true;
            }
            return false;
        }
        return false;
    }
}