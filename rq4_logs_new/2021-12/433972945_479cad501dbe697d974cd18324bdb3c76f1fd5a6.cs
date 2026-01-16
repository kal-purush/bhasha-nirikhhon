namespace AocNetLib
{
    public class Day2
    {
        public string Solve(string input)
        {
            Line[] lines = ParseInput(input);
            int hor = 0;
            int depth = 0;
            foreach (var line in lines)
            {
                switch (line.Dir)
                {
                    case "up":
                        depth -= line.Value;
                        break;
                    case "down":
                        depth += line.Value;
                        break;
                    case "forward":
                        hor += line.Value;
                        break;
                    default:
                        throw new Exception("Invalid direction");
                }
            }
            return (hor*depth).ToString();
        }

        public string Solve2(string input)
        {
            Line[] lines = ParseInput(input);
            int hor = 0;
            int depth = 0;
            int aim = 0;
            foreach (var line in lines)
            {
                switch (line.Dir)
                {
                    case "up":
                        aim -= line.Value;
                        break;
                    case "down":
                        aim += line.Value;
                        break;
                    case "forward":
                        hor += line.Value;
                        depth += line.Value * aim;
                        break;
                    default:
                        throw new Exception("Invalid direction");
                }
            }
            return (hor * depth).ToString();
        }

        private static Line[] ParseInput(string input)
        {
            return input.Split(new char[] { '\n' }, StringSplitOptions.RemoveEmptyEntries)
                    .Select(x => Line.Parse(x.Trim())).ToArray();
        }

        class Line
        {
            public string Dir { get; set; }
            public int Value { get; set; }

            public static Line Parse(string line)
            {
                var parts = line.Split(' ');
                return new Line { Dir = parts[0], Value = int.Parse(parts[1]) };
            }
        }
    }
}