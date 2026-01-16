using System;

namespace Addition
{
    public interface IAdd
    {
        dynamic Sum(dynamic a, dynamic b);
    }
    public class Adding : IAdd
    {
        private static dynamic _result;

        public static dynamic Result
        {
            get => _result;
            set => _result = value;
        }
        public static void Main()
        {
        }
        public dynamic Sum(dynamic a, dynamic b)
        {
            Result = MathOperations.Addition.Sum(a, b);
            return Result;
        }

    }
}