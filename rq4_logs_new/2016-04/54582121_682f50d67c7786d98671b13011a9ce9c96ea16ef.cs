using System;
using System.Drawing;

namespace RootNav.Data
{
	/// <summary>
	/// Summary description for Vector2D.
	/// </summary>
	public struct Vector2D
	{
		private double x, y;
		public static readonly Vector2D Empty = new Vector2D ();

		public Vector2D (double x, double y)
		{
			this.x = x;
			this.y = y;
		}

		#region Properties

		public double X {
			get { return x; }
			set { x = value; }
		}

		public double Y {
			get { return y; }
			set { y = value; }
		}

		#endregion

		#region Object

		public override bool Equals (object obj)
		{
			if (obj is Vector2D) {
				Vector2D v = (Vector2D)obj;
				if (v.x == x && v.y == y)
					return obj.GetType ().Equals (this.GetType ());
			}
			return false;
		}

		public override int GetHashCode ()
		{
			return base.GetHashCode ();
		}

		public override string ToString ()
		{
			return String.Format ("{{X={0}, Y={1}}}", x, y);
		}

		#endregion

		public double Norm ()
		{
			return Math.Sqrt (x * x + y * y);
		}

		public static bool operator== (Vector2D u, Vector2D v)
		{
			if (u.X == v.x && u.y == v.y)
				return true;
			else
				return false;
		}

		public static bool operator != (Vector2D u, Vector2D v)
		{
			return u != v;
		}

		public static Vector2D operator+ (Vector2D u, Vector2D v)
		{
			return new Vector2D (u.x + v.x, u.y + v.y);
		}

		public static Vector2D operator- (Vector2D u, Vector2D v)
		{
			return new Vector2D (u.x - v.x, u.y - v.y);
		}

		public static Vector2D operator* (Vector2D u, double a)
		{
			return new Vector2D (a * u.x, a * u.y);
		}

		public static Vector2D operator/ (Vector2D u, double a)
		{
			return new Vector2D (u.x / a, u.y / a);
		}

		public static Vector2D operator- (Vector2D u)
		{
			return new Vector2D (-u.x, -u.y);
		}

		public static double GetAngle (PointF c)
		{
			// angolo retta passante per l'origine
			return GetAngle (c, new PointF (0, 0));
		}

		public static double GetAngle (PointF c, PointF f)
		{
			// angolo in gradi tra i due punti c ed f - gradi
			//            return Math.Atan((f.X - c.X) / (f.Y - c.Y) / Math.PI * 180 );
			return (180 * (1 + Math.Atan2 ((c.Y - f.Y), (c.X - f.X)) / Math.PI));
		}

		public static double Distance (PointF a, PointF b)
		{
			return Math.Sqrt (Math.Pow ((a.X - b.X), 2) + Math.Pow ((a.Y - b.Y), 2));
		}

		public static PointF Rotate (PointF point, PointF origin, double angle)
		{
			double X = origin.X + ((point.X - origin.X) * Math.Cos (angle) -
			           (point.Y - origin.Y) * Math.Sin (angle));
			double Y = origin.Y + ((point.X - origin.X) * Math.Sin (angle) +
			           (point.Y - origin.Y) * Math.Cos (angle));
			return new PointF ((float)X, (float)Y);
		}

		public static PointF GetPoint (PointF origin, double length, double angle)
		{
			return new PointF ((float)(origin.X + length * Math.Cos (angle)),
				(float)(origin.Y + length * Math.Sin (angle)));
		}

		public static explicit operator PointF (Vector2D u)
		{
			return new PointF ((float)u.x, (float)u.y);
		}

		public static implicit operator Vector2D (PointF p)
		{
			return new Vector2D (p.X, p.Y);
		}

	}
}
