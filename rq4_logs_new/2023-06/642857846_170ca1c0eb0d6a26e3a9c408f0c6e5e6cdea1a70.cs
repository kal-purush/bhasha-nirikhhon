namespace Game
{
    class Walkable : IWalkable
    {
        public bool isWalking { get; set; }
        public KeycapMoves walkDirection { get; set; }
        public void walk(Dictionary<string, int> point)
        {
            if (this.isWalking)
            {
                switch (this.walkDirection)
                {
                    case KeycapMoves.RightArrow:
                        point["X"]++;
                        this.isWalking = false;
                        break;
                    case KeycapMoves.LeftArrow:
                        point["X"]--;
                        this.isWalking = false;
                        break;

                }

            }
        }
    }
}