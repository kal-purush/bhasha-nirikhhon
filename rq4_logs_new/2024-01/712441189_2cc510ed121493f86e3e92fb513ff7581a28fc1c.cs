namespace Threads
{
    internal class CharDictionary<T> : ThreadsForArray<T>
    {
        public Dictionary<T, int> Result { get; set; }
        
        private Dictionary<T, int>[] ResultArray { get; set; }
        
        public CharDictionary(int threadCount, T[] resultArray) : base(threadCount, resultArray)
        {
            ResultArray = new Dictionary<T, int>[threadCount];
            Console.WriteLine();
        }

        public override void Process()
        {
            base.Process();
            Result = new Dictionary<T, int>();
            foreach (var item in ResultArray)
            {
                foreach (var pair in item)
                {
                    if (Result.TryGetValue(pair.Key, out int value))
                    {
                        Result[pair.Key] = value + pair.Value;
                    }
                    else
                    {
                        Result[pair.Key] = pair.Value;
                    }
                }
            }
        }

        protected override void Callback(Span<T> span, int index, int threadIndex)
        {
            if (ResultArray[threadIndex] == null)
            {
                ResultArray[threadIndex] = ResultArray[threadIndex] = new Dictionary<T, int>();
            }

            if (ResultArray[threadIndex].TryGetValue(span[index], out int val)) {
                ResultArray[threadIndex][span[index]] = val + 1;
            } else
            {
                ResultArray[threadIndex][span[index]] = 1;
            }

        }
    }
}