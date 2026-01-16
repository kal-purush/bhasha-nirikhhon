using System;

namespace DiscreteFunctions
{
    public class Df2DOpt : Df2DChunked<double?, double?>
    {

        #region Constructors
        public Df2DOpt(double?[] nodes, double?[] values) : base(nodes == null ? null : new[] { nodes }, new[] { values })
        {
        }

        public Df2DOpt(params double?[] chunks) : base(new[] { chunks })
        {
        }
        #endregion

        public new double?[] X
        {
            get { return base.X?[0]; }
            set { base.X = value == null ? null : new[] { value }; }
        }

        public new double?[] Y
        {
            get { return base.Y?[0]; }
            set { base.Y = value == null ? null : new[] { value }; }
        }


    }
}