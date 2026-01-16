using System;
using MathOperations;
using Microsoft.CSharp;

namespace Square
{
    public interface ISquare
    {
        dynamic SquareExpo(dynamic a);
    }
    public class Squaring : ISquare
    {
        private static dynamic _result;
        public static dynamic Result
        {
            get => _result;
            set => _result = value;
        }

        public dynamic SquareExpo(dynamic a)
        {
            Result = MathOperations.Square.SquareMethod(a); ;
            return Result;
        }

    }
}