using Iterator.Models;

namespace Iterator.Iterator;

public class FoodIterator : IIterator<Food>
{
    private int _step = 1;
    private int _current = 0;
    private readonly FoodCollection _foodCollection;

    public FoodIterator(FoodCollection foodCollection)
    {
        _foodCollection = foodCollection;
    }

    public Food First()
    {
        _current = 0;
        return _foodCollection[_current];
    }

    public Food Next()
    {
        _current += _step;
        if (!IsDone)
            return _foodCollection[_current];
        else
            return null;
    }

    public Food Current => _foodCollection[_current];

    public bool IsDone => _current >= _foodCollection.Count;


    public int Step
    {
        get => _step;
        set => _step = value;
    }
}