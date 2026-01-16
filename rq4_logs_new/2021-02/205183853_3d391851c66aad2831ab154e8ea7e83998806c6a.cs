

namespace Linear_DataStructures
{
    public class PirorityQueue
    {
        private int[] a= new int [5];
        int count=0;
        //o(n)
        public void Add(int value)
        {
            if(IsFull())
            {
                System.Console.WriteLine("queue is full");
            }
            else
            {
                int index =ShiftArray(value);
                a[index]=value;
                count++;
            }
        }
        //1,3,4,5
        public int ShiftArray(int value)
        {
            int i;
            for( i=count-1;i>=0;i--)
            {
                if(value<a[i])
                {
                    a[i+1]=a[i];
                }
                else
                {
                    break;
                }
            }

            return i+1;
           
        }

        public int Remove()
        {
             if(IsEmpty())
            {
                throw new System.InvalidOperationException("empty queue");
            }
            return a[--count];
        }

        public bool IsFull()
        {
            
            return count==a.Length;
        }

        public bool IsEmpty()
        {
            return count==0;
        }

        public static void DriverCode()
        {
            PirorityQueue pirority= new PirorityQueue();
            
            pirority.Add(10);
            pirority.Add(30);
            pirority.Add(40);
            pirority.Add(50);
            pirority.Add(20);
            System.Console.WriteLine(pirority.Remove());
        }
    }
}