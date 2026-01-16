namespace AocNetLib
{
    public class Day6
    {
        public string Solve(string input, int days)
        {
            School school = ParseInput(input);
            for (int day = 0; day < days; day++)
            {
                school.Iterate();
            }
            return school.Count.ToString();
        }

        private School ParseInput(string input)
        {
            return new School(input.Split(',').Select(x => int.Parse(x)));
        }

        class School
        {
            List<int> school = new List<int>();

            public int Count => school.Count;

            public School(IEnumerable<int> list)
            {
                school.AddRange(list);
            }

            public void Iterate()
            {
                int newCnt = 0;
                for (int i = 0; i < school.Count; i++)
                {
                    if(school[i] == 0)
                    {
                        newCnt++;
                        school[i] = 6;
                    }
                    else
                    {
                        school[i]--;
                    }
                }
                for (int i = 0; i < newCnt; i++)
                {
                    school.Add(8);
                }
            }
        }
    }
}