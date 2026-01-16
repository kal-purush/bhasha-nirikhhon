namespace DotNetCafe.Internals
{
    internal static class CpfComparer
    {
        public static int Compare(Cpf lhs, object rhs)
        {
            if (!(rhs is Cpf))
            {
                return 1;
            }

            return Compare(lhs, (Cpf) rhs);
        }

        public static int Compare(Cpf lhs, Cpf rhs)
        {
            long x = lhs.number;
            long y = rhs.number;

            if (x > y)
            {
                return 1;
            }

            if (x < y)
            {
                return -1;
            }

            return 0;
        }
    }
}