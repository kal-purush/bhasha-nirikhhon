using System;
using MathOperations;
using Microsoft.CSharp;

namespace Square
{
    public interface ISquare
    {
        dynamic Square(dynamic a);
    }
    public class Square : ISquare
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
        public dynamic Difference(dynamic a)
        {
            Result = MathOperations.Squares.Square(a); ;
            return Result;
        }

    }
}