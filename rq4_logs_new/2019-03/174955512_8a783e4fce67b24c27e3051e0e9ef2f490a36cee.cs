public class IndexCounter
{
    int size;
    int index;
    public int Index{
        get => index;
        set {
            index = (value + size) % size;
        }
    }

    public IndexCounter(int _size, int _index = 0){
        index = _index;
        size = _size;
    }

    public void Increment(){
        Index++;
    }

    public void Decrement(){
        Index--;
    }

    public int GetNext(){
        return (index + 1 + size) % size;
    }

    public int GetPrev(){
        return (index - 1 + size) % size;
    }
}