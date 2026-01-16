public class ArrayList
{
    private int[] _array;
    private int _size;

    public int Count;

    public ArrayList()
    {
        _array = new int[10];
        Count = 0;
    }

    public ArrayList(int element)
    {
        _array = new int[10];
        _array[0] = element;
        Count = 1;
    }

    public ArrayList(int[] elements)
    {
        _array = new int[(int) (elements.length * 1.5)];
        System.arraycopy(elements, 0, _array, 0, elements.length);
        Count = elements.length;
    }
    public int Get(int index)
    {
        if (index < 0 || index >= Count){
            throw new IndexOutOfBoundsException("Bad index");
        }
        return _array[index];
    }
}