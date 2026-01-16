using System;
using MathOperations;
using Microsoft.CSharp;

namespace Multiplication
{
    public interface IMultiply
    {
        dynamic Product(dynamic a, dynamic b);
    }
    public class Multiplying : IMultiply
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
        public dynamic Product(dynamic a, dynamic b)
        {
            Result = MathOperations.Multiplication.Product(a, b); ;
            return Result;
        }

    }
}