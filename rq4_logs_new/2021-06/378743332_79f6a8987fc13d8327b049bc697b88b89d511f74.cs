namespace SnlGame.Model
{
    class Token
    {
        private int _Position;

        public int Position
        {
            get
            {
                return _Position;
            }
            set
            {
                if (value >= 0 && value <= 100)
                {
                    _Position = value;
                }
            }
        }

        public bool isAWinnerPosition
        {
            get
            {
                return _Position == 100;
            }
        }
    }
}