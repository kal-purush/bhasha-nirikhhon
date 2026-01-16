using System;

namespace CompteConnect
{
    public class Position
    {
        protected bool Equals(Position other)
        {
            return Row == other.Row && Column == other.Column;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((Position) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (Row*397) ^ Column;
            }
        }

        public Position(int row, int column)
        {
            Row = row;
            Column = column;
        }

        public int Row { get; private set; }
        public int Column { get; private set; }

        public static bool operator ==(Position p1, Position p2)
        {
            if (ReferenceEquals(p1, p2))
            {
                return true;
            }
            if (ReferenceEquals(p1, null) || ReferenceEquals(p2, null))
            {
                return false;
            }
            return p1.Row == p2.Row && p1.Column == p2.Column;

        }

        public static bool operator !=(Position p1, Position p2)
        {
            return !(p1 == p2);
        }

        public override string ToString()
        {
            return string.Format("Row: {0}，Column: {1}", Row, Column);
        }
    }
}