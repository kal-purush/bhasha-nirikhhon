using System;
using System.Collections.Generic;
using System.Text;

namespace Module_5
{
    class PositionOnThePlane
    {
        // В соответствии со статьей на хабре, имя открытой переменной - в Pascal casing.
        public int X { get; set; } = 0;
        public int Y { get; set; } = 0;

        public PositionOnThePlane(int x, int y)
        {
            X = x;
            Y = y;
        }

        public static bool operator ==(PositionOnThePlane position1, PositionOnThePlane position2)
        {
            return (position1.X == position2.X && position1.Y == position2.Y) ? true : false;

        }

        public static bool operator !=(PositionOnThePlane position1, PositionOnThePlane position2)
        {
            return (position1.X == position2.X && position1.Y == position2.Y) ? false : true;

        }

        // Написал эти методы хотя и не использую их, т.к. вылетало предупреждение.
        public override bool Equals(object obj)
        {
            if (obj.GetType() != this.GetType())
            {
                return false;
            }

            PositionOnThePlane position = (PositionOnThePlane)obj;
            return (this.X == position.X && this.Y==position.Y);
        }

        public override int GetHashCode()
        {
            return X.GetHashCode() + Y.GetHashCode();
        }
    }
}