/*
 * Copyright © 2018, Jon Hood www.hoodsecurity.com
 * License: ISC
 */

using System;

namespace BadWeakness
{
	class Bad
	{
		static void Main()
		{
			unsafe
			{
				bool* p = null;
				System.Console.WriteLine(*p);
			}
		}
	}
}