namespace Linear_DataStructures
{
    public class MaxStockProfit
    {
        public void GetMaxStockProfit(int[] s)
        {
            int maxProfit=0;
            int day1=0;
            int day2=0;
            int sum;

            for(int i=0;i<s.Length-1;i++)
            {
                sum=s[i+1]-s[i];

                if(sum>0)
                {
                    System.Console.WriteLine(sum) ;
                    if(maxProfit==0)
                    {
                          day1=i;
                    }
                    maxProfit+=sum;
                    day2=i+1;
                }
                else
                {
                    System.Console.WriteLine("day1 {0}  day 2 {1}",day1.ToString(),day2.ToString());
                    maxProfit=0;
                    day1=i+1;
                }
                 
            }
            System.Console.WriteLine("day1 {0}  day 2 {1}",day1.ToString(),day2.ToString());
        }

        public static  void DriverCode(){

            MaxStockProfit m = new MaxStockProfit();
            int[] a={100, 180, 260, 310, 40, 535, 695};
            
            m.GetMaxStockProfit(a);

        }
    }
}