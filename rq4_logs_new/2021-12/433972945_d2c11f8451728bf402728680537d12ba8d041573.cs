namespace AocNetLib
{
    public class Day21
    {
        int p1Pos;
        int p2Pos;
        int p1Score;
        int p2Score;
        int rollCnt;
        int dice;

        public string Solve(string v)
        {
            ParseInput(v);
            while(true)
            {
                if (StepPlayer(ref p1Pos, ref p1Score, RollDice3())) break;
                if (StepPlayer(ref p2Pos, ref p2Score, RollDice3())) break;
                Console.WriteLine($"[{rollCnt,5}] p1:{p1Pos} ({p1Score}) p2:{p2Pos} ({p2Score})");
            }
            int score = (p1Score >= 1000) ? p2Score : p1Score;
            return (score * rollCnt).ToString();
        }

        private bool StepPlayer(ref int pos, ref int score, int dice)
        {
            pos = (pos + dice - 1) % 10 + 1;
            score += pos;
            return score >= 1000;
        }

        private int RollDice()
        {
            dice++;
            if (dice > 100) dice = 1;
            rollCnt++;
            return dice;
        }

        private int RollDice3()
        {
            return RollDice() + RollDice() + RollDice();
        }

        private void ParseInput(string input)
        {
            var lines = input.TrimEnd().Split(new char[] { '\n' }).Select(x => x.Trim()).ToArray();
            p1Pos = int.Parse(lines[0].Split(':').Last().Trim());
            p2Pos = int.Parse(lines[1].Split(':').Last().Trim());
        }
    }
}