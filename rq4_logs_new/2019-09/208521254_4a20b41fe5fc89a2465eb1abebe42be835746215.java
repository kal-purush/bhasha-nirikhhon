public class arrayGenerator
{
    public void main(String[] args)
    {
        //Config
        int size = 10;
        //Generate It
        a = arrayGen(size);
    }
    //Creates an array of the specifed size
    public int[] arrayGen(int size)
    {
        int[] a = new int[size];
            for(int i = 0; i < 3; i++)
            {
                a[i] = readInt("Input a number: ");
            }
        return a;
    }
}