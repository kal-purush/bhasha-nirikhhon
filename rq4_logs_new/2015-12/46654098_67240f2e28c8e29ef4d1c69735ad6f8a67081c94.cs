using System;
using System.Collections.Generic;
using System.Drawing;
using System.Drawing.Imaging;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TagCloud
{
	public class ImageSaver : ISaver
	{
		public Dictionary<string, ImageFormat> Formats = new Dictionary<string, ImageFormat>
		{
			{"png", ImageFormat.Png},
			{"jpeg", ImageFormat.Jpeg},
			{"gif", ImageFormat.Gif},
			{"bmp", ImageFormat.Bmp}
		};

		public void SaveImage(Bitmap image, string fileName, Config config)
		{
			var name = fileName + '.' + config.ConfigModel.ImageFormat;
			image.Save(name, Formats[config.ConfigModel.ImageFormat]);
			image.Dispose();
		}
	}
}