using StackNamespace;

namespace CalculatorNamespace
{
    class Calculator
    {
        public void SumUp()
        {
            int first = stack.Top();
            stack.Pop();
            int second = stack.Top();
            stack.Pop();
            stack.Push(first + second);
        }

        public void Subtract()
        {
            int first = stack.Top();
            stack.Pop();
            int second = stack.Top();
            stack.Pop();
            stack.Push(second - first);
        }

        public void Multiply()
        {
            int first = stack.Top();
            stack.Pop();
            int second = stack.Top();
            stack.Pop();
            stack.Push(first * second);
        }

        public void Divide()
        {
            int first = stack.Top();
            stack.Pop();
            int second = stack.Top();
            stack.Pop();
            stack.Push(second / first);
        }

        void Push(int value)
        {
            stack.Push(value);
        }

        int Result() => stack.Top();

        private Stack stack;
    }
}