using System.Collections;
using System.Collections.Generic;

namespace CombatWorld.Utility
{
	public class Vec2i
	{
		public int x { get; private set; }
		public int y { get; private set; }

		public Vec2i(int x, int y)
		{
			this.x = x;
			this.y = y;
		}

		public Vec2i Get()
		{
			return this;
		}

		public static Vec2i operator +(Vec2i left, Vec2i right)
		{
			return new Vec2i(left.x + right.x, left.y + right.y);
		}

		public static Vec2i operator -(Vec2i left, Vec2i right)
		{
			return new Vec2i(left.x - right.x, left.y - right.y);
		}

		public override string ToString()
		{
			return "{" + x + "," + y + "}";
		}
	}
}