using System;

namespace ClassesTasks
{
    public class PlayingWithCubes2
    {
        public class Cube
        {
            private int Side;
            public int GetSide() { return this.Side; }
            public void SetSide(int s) { this.Side = Math.Abs(s); }

            public Cube() { Side = 0; }

            public Cube(int num) { Side = Math.Abs(num); }
            
            // private int _side;
            // public int GetSide() => _side;
            // public void SetSide(int s) => _side = System.Math.Abs(s);
            // public Cube(int side = 0) => _side = System.Math.Abs(side);
        }
    }
}