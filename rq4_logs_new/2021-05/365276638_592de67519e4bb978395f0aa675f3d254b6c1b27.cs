using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Drawing;
using System.Globalization;

namespace BlightnautsDialogue
{
    public static class Settings
    {
        private static string Path { get => Program.Path + "\\config.ini"; }
        private const string fontSize = "FONTSIZE=";
        private const string foregroundColor = "FOREGROUNDCOLOR=";
        private const string backgroundColor = "BACKGROUNDCOLOR=";

        public static int Save(float zoom, Color fore, Color back)
        {
            string content = fontSize + zoom.ToString("F1", CultureInfo.InvariantCulture);
            content += string.Format
            (
                "\n{0}{1} {2} {3}",
                foregroundColor,
                fore.R.ToString(CultureInfo.InvariantCulture),
                fore.G.ToString(CultureInfo.InvariantCulture),
                fore.B.ToString(CultureInfo.InvariantCulture)
            );
            content += string.Format
            (
                "\n{0}{1} {2} {3}",
                backgroundColor,
                back.R.ToString(CultureInfo.InvariantCulture),
                back.G.ToString(CultureInfo.InvariantCulture),
                back.B.ToString(CultureInfo.InvariantCulture)
            );

            try
            {
                File.WriteAllText(Path, content);
            }
            catch
            {
                return 1;
            }

            return 0;
        }

        public static int Load(out float zoom, out Color fore, out Color back)
        {
            zoom = 12;
            fore = System.Windows.Forms.TextBox.DefaultForeColor;
            back = System.Windows.Forms.TextBox.DefaultBackColor;

            if (!File.Exists(Path))
                return 1;

            try
            {
                string[] content = File.ReadAllLines(Path);

                for (int i = 0; i < content.Length; i++)
                {
                    if (content[i].StartsWith(fontSize))
                    {
                        zoom = float.Parse(content[i].Substring(fontSize.Length), CultureInfo.InvariantCulture);
                    }
                    else if (content[i].StartsWith(foregroundColor))
                    {
                        string[] colors = content[i].Substring(foregroundColor.Length).Split(' ');
                        int r = int.Parse(colors[0], CultureInfo.InvariantCulture);
                        int g = int.Parse(colors[1], CultureInfo.InvariantCulture);
                        int b = int.Parse(colors[2], CultureInfo.InvariantCulture);
                        fore = Color.FromArgb(r, g, b);
                    }
                    else if (content[i].StartsWith(backgroundColor))
                    {
                        string[] colors = content[i].Substring(backgroundColor.Length).Split(' ');
                        int r = int.Parse(colors[0], CultureInfo.InvariantCulture);
                        int g = int.Parse(colors[1], CultureInfo.InvariantCulture);
                        int b = int.Parse(colors[2], CultureInfo.InvariantCulture);
                        back = Color.FromArgb(r, g, b);
                    }
                }
            }
            catch
            {
                return 2;
            }

            return 0;
        }
    }
}