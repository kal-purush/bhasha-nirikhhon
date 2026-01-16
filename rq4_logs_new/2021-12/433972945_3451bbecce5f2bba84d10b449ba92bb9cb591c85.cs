namespace AocNetLib
{
    public class Day4
    {
        public string Solve(string input)
        {
            var game = ParseInput(input);
            int result;
            do
            {
                game.Iterate();
                result = game.GetWinner();
            }while (result == 0);
            return result.ToString();
        }


        static Game ParseInput(string input)
        {
            var lines = input.Split(new char[] { '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries).Select(x => x.Trim()).ToArray();
            var nums = lines[0].Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries).Select(x => int.Parse(x.Trim())).ToArray();
            var game = new Game(nums);
            for (int i = 1; i < lines.Length; i += Table.TableSize)
            {
                var table = new Table();
                for (int j = 0; j < Table.TableSize; j++)
                {
                    var row = lines[i + j].Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries).Select(x => int.Parse(x.Trim())).ToArray();
                    for (int k = 0; k < Table.TableSize; k++)
                    {
                        table.Numbers[j, k] = row[k];
                    }
                }
                game.Tables.Add(table);
            }
            return game;
        }

        class Table
        {
            public const int TableSize = 5;

            public readonly int[,] Numbers;

            readonly bool[,] marks;

            public Table()
            {
                Numbers = new int[TableSize, TableSize];
                marks = new bool[TableSize, TableSize];
            }

            public void Mark(int num)
            {
                for (int i = 0; i < TableSize; i++)
                {
                    for (int j = 0; j < TableSize; j++)
                    {
                        if (Numbers[i, j] == num)
                        {
                            marks[i, j] = true;
                        }
                    }
                }
            }

            public bool IsWinning()
            {
                for (int i = 0; i < TableSize; i++)
                {
                    bool isRowMarked = true;
                    bool isColMarked = true;
                    for (int j = 0; j < TableSize; j++)
                    {
                        if (!marks[i, j]) isRowMarked = false;
                        if (!marks[j, i]) isColMarked = false;
                    }
                    if(isRowMarked || isColMarked) return true;
                }
                return false;
            }

            public int GetValue()
            {
                int sum = 0;
                for (int i = 0; i < TableSize; i++)
                {
                    for (int j = 0; j < TableSize; j++)
                    {
                        if (!marks[i, j]) sum += Numbers[i, j];
                    }
                }
                return sum;
            }
        }

        public string Solve2(string input)
        {
            var game = ParseInput(input);
            while(game.Tables.Count > 1)
            {
                game.Iterate();
                game.EliminateWinners();
            }
            while(!game.Tables[0].IsWinning())
            {
                game.Iterate();
            }
            int result = game.GetWinner();
            return result.ToString();
        }

        class Game
        {
            public readonly List<Table> Tables;
            int[] numbers;
            int round;
            int lastNum;

            public Game(int[] nums)
            {
                Tables = new List<Table>();
                numbers = nums;
                round = 0;
                lastNum = 0;
            }

            public void Iterate()
            {
                lastNum = numbers[round++];
                List<Table> winners = new List<Table>();
                foreach (var table in Tables)
                {
                    table.Mark(lastNum);
                }
            }

            public int GetWinner()
            {
                foreach (var table in Tables)
                {
                    if (table.IsWinning())
                    {
                        int value = table.GetValue();
                        return value * lastNum;
                    }
                }
                return 0;
            }

            public void EliminateWinners()
            {
                Tables.RemoveAll(x => x.IsWinning());
            }
        }
    }
}