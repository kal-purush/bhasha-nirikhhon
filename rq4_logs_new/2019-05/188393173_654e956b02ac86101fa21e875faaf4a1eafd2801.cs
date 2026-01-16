using System;
using System.Xml;

namespace Amaze.Parse
{
	public class Parser
	{
		public static int[,] Parse (string path)
		{
			var reader = new XmlTextReader (path);
			reader.MoveToContent ();

			// parse size
			reader.ReadToDescendant ("layer");
			var widthString = reader.GetAttribute ("width");
			var heightString = reader.GetAttribute ("height");
			int.TryParse (widthString, out var width);
			int.TryParse (heightString, out var height);
			if (width <= 0 || height <= 0) {
				return null;
			}

			// parse data
			reader.ReadToDescendant ("data");
			var dataString = reader.ReadElementString ("data").Trim ();
			var data = ParseData (width, height, dataString);
			return data;
		}

		private static int[,] ParseData (int width, int height, string data)
		{
			var datas = new int[height, width];

			var dataStringArr = data.Split (new[] { '\n', ',' }, StringSplitOptions.RemoveEmptyEntries);

			for (int y = 0, i = 0; y < height && i < dataStringArr.Length; y++) {
				for (var x = 0; x < width && i < dataStringArr.Length; x++, i++) {
					datas [y, x] = dataStringArr [i] == "0" ? 0 : 1;
				}
			}

			return datas;
		}
	}
}