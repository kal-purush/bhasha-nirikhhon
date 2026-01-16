using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MusicPlayer
{
	public abstract class Skins
	{
		public abstract void Clear();
		public abstract void Render(string text);
		public void Render()
		{
			Console.WriteLine();
		}
	}

	public class ClassicSkin: Skins
	{
		public override void Clear()
		{
			Console.Clear();
		}

		public override void Render(string text)
		{
			Console.WriteLine(text);
		}
	}

	public class ColoredSkin : Skins
	{
		private ConsoleColor textColor;

		public ColoredSkin(ConsoleColor tmpColor)
		{
			textColor = tmpColor;
		}

		public override void Clear()
		{
			Console.Clear();
		}

		public override void Render(string text)
		{
			Console.ForegroundColor = textColor;
			Console.WriteLine(text);
			Console.ResetColor();
		}
	}

	public class RandColorSkin : Skins
	{
		Random rand = new Random();

		public override void Clear()
		{
			Console.Clear();
			int i = 30;
			while (i > 0)
			{
				Console.Write("\u058d");
				i--;
			}
			Console.WriteLine();
		}

		public override void Render(string text)
		{
			Console.ForegroundColor = (ConsoleColor)rand.Next(15);
			string reversedString = new string(
				text.Select(c => char.IsLower(c) ? char.ToUpper(c) : char.ToLower(c)).ToArray());
			Console.WriteLine(reversedString);
			Console.ResetColor();
		}
	}
}