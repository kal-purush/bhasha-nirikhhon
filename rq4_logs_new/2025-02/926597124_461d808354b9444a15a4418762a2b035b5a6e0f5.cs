using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using System.Windows.Controls;

namespace slightly_cooler_sound_board
{
    internal class WorkingWithFiles
    {
        public static void CreateSaveFile(StackPanel stack)
        {
            Trace.WriteLine(stack.Children.OfType<Button>().ToString());

            ArrayList blocks = new ArrayList(stack.Children.OfType<MusicBlock>().ToList());
        string json= JsonSerializer.Serialize(blocks);
            Trace.WriteLine(blocks[0]);
            File.WriteAllText("saved sounds", json);
        }

        public static void ReadSaveFile()
        {

        }
    }
}