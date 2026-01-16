namespace Iterator.Iterator;

public class Iterator : IIterator<Food>
{
    private readonly Collection _collection;
    int current = 0;
    int step = 1;

    public Iterator(Collection collection)
    {
        this._collection = collection;
    }

    public Food First()
    {
        current = 0;
        return _collection[current];
    }

    public Food Next()
    {
        current += step;
        if (!IsDone)
            return _collection[current];
        else
            return null;
    }


    public int Step
    {
        get => step;
        set => step = value;
    }

    public Food Current => _collection[current] ;

    public bool IsDone => current >= _collection.Count;
}