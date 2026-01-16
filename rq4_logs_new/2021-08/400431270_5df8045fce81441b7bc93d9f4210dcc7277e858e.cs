using PdfSharp.Fonts;
using System.Diagnostics;
using System.IO;

namespace cs_con_framework_pdfsharp {
    class FontResolver : IFontResolver {
		byte[] IFontResolver.GetFont(string faceName)
		{
			switch (faceName)
			{
				case "けいふぉんと":
					return LoadFontData("keifont.ttf");
			}

			return null;
		}
		private byte[] LoadFontData(string resourceName)
		{
			try
			{
				using (FileStream stream = new FileStream(resourceName, FileMode.Open, FileAccess.Read))
				{
					byte[] data = new byte[stream.Length];
					stream.Read(data, 0, (int)stream.Length);
					return data;
				}
			}
			catch (FileNotFoundException ex)
			{
				Debug.WriteLine(ex.Message);
			}

			return null;
		}

		FontResolverInfo IFontResolver.ResolveTypeface(string familyName, bool isBold, bool isItalic)
		{
			switch (familyName)
			{
				case "けいふぉんと":
					return new FontResolverInfo("けいふぉんと");
			}

			return null;
		}
	}
}