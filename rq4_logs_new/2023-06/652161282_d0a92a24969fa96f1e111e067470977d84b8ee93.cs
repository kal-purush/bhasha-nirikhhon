using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Recipe_Organizer_PRN211.Utility
{
	public static class Base64
	{
		public static Image Base64ToImage(string base64String)
		{
			byte[] imageBytes = Convert.FromBase64String(base64String);
			MemoryStream ms = new MemoryStream(imageBytes, 0, imageBytes.Length);
			ms.Write(imageBytes, 0, imageBytes.Length);
			System.Drawing.Image image = System.Drawing.Image.FromStream(ms, true);
			return image;
		}



		//Transfer Img to base 64 
		public static string ImageToBase64(string path)
		{
			// string path = "D:\SampleImage.jpg";
			using (System.Drawing.Image image = System.Drawing.Image.FromFile(path))
			{
				using (MemoryStream m = new MemoryStream())
				{
					image.Save(m, image.RawFormat);
					byte[] imageBytes = m.ToArray();
					string base64String = Convert.ToBase64String(imageBytes);
					return base64String;
				}
			}
		}
	}
}