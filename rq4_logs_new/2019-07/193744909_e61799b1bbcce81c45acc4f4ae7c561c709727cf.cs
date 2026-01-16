using Vector = MathNet.Numerics.LinearAlgebra.Vector<System.Double>;
using Matrix = MathNet.Numerics.LinearAlgebra.Matrix<System.Double>;
using System.Linq;
namespace StepRedraw.Math
{
    public class Line
    {

        const int x = 0, y = 1;
        readonly Vector _canonicalForm;
        Line(Vector canonicalForm)
        {
            _canonicalForm = canonicalForm;
        }

        public Vector CanonicalForm => _canonicalForm;
        public static Line Create(Vector canonicalForm)
        {
            return new Line(canonicalForm);
        }

        public static Line Create(Vector A, Vector B)
        {
            // a*x+b*y+c=0
            // a =  (B[y]-A[y])
            // b = -(B[x]-A[x])
            // c =  A[y]*(-b) - A[x]*a

            var a = (B[y] - A[y]);
            var b = -(B[x] - A[x]);
            var c = A[y] * (-b) - A[x] * a;

            var ret = new Line(Vector.Build.Dense(new[] { a, b, c }));
            return ret;

        }

        public Line PerpendicularThroughThePoint(Vector C)
        {
            // a*x+b*y+c=0
            var a = _canonicalForm[0];
            var b = _canonicalForm[1];
            var with_k = (0.0D == b ? _canonicalForm : CanonicalForm / b).Clone(); // to k*x + y + c/b =0
            if (0.0D != a && 0.0D != b)
            {
                with_k[0] =  -1 / with_k[0]; //rotate 90 deg
                with_k[1] = with_k[1];
                with_k[2] = -C[y] - with_k[0] * C[x]; // y - C[y] = k*(x - C[x]) => k*x - y - k*C[x] + C[y] = 0
            }
            else
            {
                if (0.0 == a) { with_k[0] = 1; with_k[1] = 0; with_k[2] = -C[x]; }
                if (0.0 == b) { with_k[0] = 0; with_k[1] = 1; with_k[2] = -C[y]; }
                // y - C[y] = k*(x - C[x]) => k*x - y - k*C[x] + C[y] = 0
            }
            if (a != 0) with_k *= a;
            var ret = new Line(with_k);
            return ret;
        }
        public Vector Intersect(Line line)
        {
            var m = Matrix.Build.DenseOfRowVectors(this.CanonicalForm, line.CanonicalForm);
            var m_param = Matrix.Build.DenseOfColumnVectors(m.Column(0), m.Column(1));
            var ret = m_param.Solve(m.Column(2) * -1);
            return ret;

        }

        public class VectorEqualityComparer : System.Collections.Generic.IEqualityComparer<Vector>
        {
            public bool Equals(Vector x, Vector y)
            {
                var ret = object.ReferenceEquals(x, y)
                    || 1 == Matrix.Build.DenseOfRowVectors(x, y).Svd().Rank; // both vectors should not be Linear independent
                return ret;
            }

            public int GetHashCode(Vector obj)
            {
                var ret = 0;
                if (obj != null)
                {
                    ret = obj.Aggregate(ret, (acc, val) => acc ^ val.GetHashCode());

                }
                return ret;
            }
        }
        public class LineEqualityComparer : System.Collections.Generic.IEqualityComparer<Line>
        {
            VectorEqualityComparer comparer = new VectorEqualityComparer();
            public bool Equals(Line x, Line y)
            {
                var ret = object.ReferenceEquals(x, y) || comparer.Equals(x.CanonicalForm, y.CanonicalForm);
                return ret;
            }

            public int GetHashCode(Line obj)
            {
                var ret = 0;
                if (obj != null)
                {
                    ret = comparer.GetHashCode(obj.CanonicalForm);

                }
                return ret;
            }
        }

    }
}