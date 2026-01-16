using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Economic
{
	/// <summary>
	/// Working with Google Spreadsheets
	/// </summary>
	public class GSWorker
	{
		private static GSWorker instance = null;
		public static GSWorker Instance
		{
			get
			{
				if(instance == null)
				{
					instance = new GSWorker();
				}

				return instance;
			}
		}

		private const string spreadheetId = "1FJkv8VYSzDotphPZvSirmt8KUGRfQUP3osD2DdM5aw0";

		public int TestPlayerBalance = 5000;
	}
}