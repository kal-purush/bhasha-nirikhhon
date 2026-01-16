using System;
using System.IO;
using System.Text;

namespace Program
{
class ConsoleApplication
{
    const string fileName = "AppSettings.dat";

  
    public static void WriteDefaultValues()
    {
        using (var stream = File.Open(fileName, FileMode.Create))
        {
            using (var writer = new BinaryWriter(stream, Encoding.UTF8, false))
            {
                writer.Write(1.250F);
                writer.Write(@"This data");
                writer.Write(10);
                writer.Write(true);
            }
        }
    }

    public static void DisplayValues()
    {
        float aspectRatio;
        string tempDirectory;
        int autoSaveTime;
        bool showStatusBar;

        if (File.Exists(fileName))
        {
            using (var stream = File.Open(fileName, FileMode.Open))
            {
                using (var reader = new BinaryReader(stream, Encoding.UTF8, false))
                {
                    aspectRatio = reader.ReadSingle();
                    tempDirectory = reader.ReadString();
                    autoSaveTime = reader.ReadInt32();
                    showStatusBar = reader.ReadBoolean();
                }
            }

            Console.WriteLine("Aspect ratio set to: " + aspectRatio);
            Console.WriteLine("Temp directory is: " + tempDirectory);
            Console.WriteLine("Auto save time set to: " + autoSaveTime);
            Console.WriteLine("Show status bar: " + showStatusBar);
        }
    }
}
}