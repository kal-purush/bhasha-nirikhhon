using Bands.Output;

namespace Bands.GettingStarted
{
    public class CounterPayload : WritablePayload,  IIncrementablePayload
    {
        public int Counter { get; private set; }

        public void Decrement()
        {
            Counter--;
        }

        public void Increment()
        {
            Counter++;
        }

        public void AddTwo()
        {
            Counter += 2;
        }

        public override string ToString()
        {
            return string.Format("Counter: {0}", Counter);
        }
    }
}