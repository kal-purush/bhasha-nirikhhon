using System;
using Calculator.Add.Interface;
using Calculator.Multiply.Interface;

namespace Calculator.Multiply.Service
{
    public class MultiplyService : IMultiplyService
    {
        private readonly IAddService _addService;

        public MultiplyService(IAddService addService)
        {
            _addService = addService;
        }

        public int Multiply(int a, int b)
        {
            var result = 0;

            for (var i = 0; i < a; i++)
            {
                result += _addService.Add(result, b);
            }

            return result;
        }
    }
}