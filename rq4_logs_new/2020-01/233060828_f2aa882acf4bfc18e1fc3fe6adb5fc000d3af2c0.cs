namespace operations
{
    public class Addition
    {
        public static int Sum(int a, int b)
        {
            return a + b;
        }

        public static double Sum(double a, double b)
        {
            return a + b;
        }

        public static double Sum(double[] arrayA)
        {
            double c = 0.0;
            foreach (double b in arrayA)
            {
                c = Sum(c, b);
            }

            return c;
        }

        public static int Sum(int[] arrayA)
        {
            int c = 0;
            foreach (int b in arrayA)
            {
                c = Sum(c, b);
            }

            return c;
        }
    }
}