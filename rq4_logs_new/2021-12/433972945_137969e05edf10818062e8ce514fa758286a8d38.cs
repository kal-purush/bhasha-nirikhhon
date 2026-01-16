namespace AocNetLib
{
    public class Day17
    {
        public string Solve(string v)
        {
            //target area: x=20..30, y=-10..-5
            var parts = v.Split(' ');
            var yLimits = parts[3].Substring(2).Split("..").Select(x=>int.Parse(x)).ToArray();
            int maxDY = Math.Min(yLimits[0], yLimits[1]);
            int maxSpeed = -maxDY - 1;
            int maxHeigh = Enumerable.Range(1, maxSpeed).Sum();
            return maxHeigh.ToString();
        }

        public string Solve2(string v)
        {
            var parts = v.Split(' ');
            var xLimits = parts[2].Substring(2, parts[2].Length - 3).Split("..").Select(x => int.Parse(x)).ToArray();
            var yLimits = parts[3].Substring(2).Split("..").Select(x => int.Parse(x)).ToArray();
            int xMax = xLimits[1];
            int xMin = GetXMin(xLimits[0]);
            int yMax = -yLimits[0] - 1;
            int yMin = yLimits[0];
            int cnt = 0;
            for(int x = xMin; x <= xMax; x++)
            {
                for (int y = yMin; y <= yMax; y++)
                {
                    if(CheckXY(x,y,xLimits, yLimits))
                    {
                        cnt++;
                    }
                }
            }
            return cnt.ToString();
        }

        private bool CheckXY(int xSpeed, int ySpeed, int[] xLimits, int[] yLimits)
        {
            int x = 0;
            int y = 0;
            int xStep = x < 0 ? -1 : 1;
            while (y >= yLimits[0] && x <= xLimits[1])
            {
                if (y >= yLimits[0] && y <= yLimits[1] && x >= xLimits[0] && x <= xLimits[1]) return true;
                y += ySpeed;
                ySpeed--;
                x += xSpeed;
                if (xSpeed != 0) xSpeed -= xStep;
            }
            return false;
        }

        private int GetXMin(int min)
        {
            int speed = 0;
            int dist = 0;
            while (dist < min) dist += ++speed;
            return speed;
        }
    }
}