namespace Operators
{
    internal class Block
    {
        private readonly int _a, _b, _c, _d;

        public Block(int a, int b, int c, int d)
        {
            _a = a;
            _b = b;
            _c = c;
            _d = d;
        }

        public override string ToString()
        {
            return string.Format("{0}, {1}, {2}, {3}", _a, _b, _c, _d);
        }

        public static bool operator ==(Block b1, Block b2)
        {
            return b1.Equals(b2);
        }

        public static bool operator !=(Block b1, Block b2)
        {
            return !b1.Equals(b2);
        }

        public override bool Equals(object item)
        {
            Block block = item as Block;
            if (block is Block)
            {
                if (block._a == _a &&
                    block._b == _b &&
                    block._c == _c &&
                    block._d == _d)
                    return true;
            }
            return false;
        }

        public override int GetHashCode()
        {
            return this.ToString().GetHashCode();
        }
    }
}