using System.Collections.Generic;
using System.IO;

namespace BlightnautsDialogue
{
    public static class ImageLoader
    {
        private static List<string> images = new List<string>();
        public static string[] Images { get => images.ToArray(); }
        public static string Default { get; private set; }
        public static bool DefaultValid { get => File.Exists(Program.Path + "\\images\\" + Default + ".dds"); }

        public static void LoadImages()
        {
            string directory = Program.Path + "\\images";
            foreach (string file in Directory.GetFiles(directory, "*.dds"))
            {
                images.Add(Path.GetFileNameWithoutExtension(file));
            }
            if (File.Exists(directory + "\\default.txt"))
            {
                Default = File.ReadAllText(directory + "\\default.txt");
            }
        }
    }
}