namespace Advent2017.Solutions
{
    class Day5
    {
        public int Solve(int[] input)
        {
            int steps = 0;
            int index = 0;
            while (index >= 0 && index < input.Length)
            {
                steps++;
                var next = input[index];
                input[index]++;
                index += next;
            }
            return steps;
        }

        public int SolvePart2(int[] input)
        {
            int steps = 0;
            int offset = 0;
            while (offset >= 0 && offset < input.Length)
            {
                steps++;
                var next = input[offset];
                if (input[offset] >= 3)
                {
                    input[offset]--;
                }
                else
                {
                    input[offset]++;
                }
                offset += next;
                
            }
            return steps;
        }
    }
}