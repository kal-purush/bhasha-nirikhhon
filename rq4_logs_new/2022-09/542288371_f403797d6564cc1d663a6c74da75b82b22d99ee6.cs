using System.Drawing;
using System.Threading.Channels;

namespace VisualCalender
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Console.Clear();
            DrawCalenderOutLine();
            DrawCalenderSystem();
            Console.ReadLine();
        }

        static void DrawCalenderSystem()
        {
            (int, int)[] iBoxCoords = new (int, int)[6];

            // Box locations for FM, Starting onsdag -> torsdag -> fredag
            iBoxCoords[0] = (5, 2);
            iBoxCoords[1] = (23, 2);
            iBoxCoords[2] = (41, 2);

            // Box locations for FM, Starting onsdag -> torsdag -> fredag
            iBoxCoords[3] = (5, 8);
            iBoxCoords[4] = (23, 8);
            iBoxCoords[5] = (41, 8);


            // Replace className input to set class
            // Draw for FM classes
            DrawCalenderBox("C#", iBoxCoords[0]); // onsdag
            DrawCalenderBox("C#", iBoxCoords[1]); // torsdag
            DrawCalenderBox("DB", iBoxCoords[2]); // fredag

            // Draw for EM classes
            DrawCalenderBox("C#", iBoxCoords[3]); // onsdag
            DrawCalenderBox("DB", iBoxCoords[4]); // torsdag
            DrawCalenderBox("DB", iBoxCoords[5]); // fredag

        }

        // Handles the structure of the calender
        static void DrawCalenderOutLine()
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("          onsdag            torsdag           fredag     ");
            Console.WriteLine("----|-----------------|-----------------|------------------");
            Console.WriteLine("    |                 |                 |                 ");
            Console.WriteLine("    |                 |                 |                 ");
            Console.WriteLine(" FM |     LEDIG       |      LEDIG      |      LEDIG      ");
            Console.WriteLine("    |                 |                 |                 ");
            Console.WriteLine("    |                 |                 |                 ");
            Console.WriteLine("----|-----------------|-----------------|------------------");
            Console.WriteLine("    |                 |                 |                 ");
            Console.WriteLine("    |                 |                 |                 ");
            Console.WriteLine(" EM |     LEDIG       |      LEDIG      |      LEDIG      ");
            Console.WriteLine("    |                 |                 |                 ");
            Console.WriteLine("    |                 |                 |                 ");
            Console.WriteLine("----|-----------------|-----------------|------------------");
            Console.WriteLine(" FM = 09:00 till 12:00");
            Console.WriteLine(" EM = 13:00 till 16:00");
            Console.ResetColor();
        }

        // Handles some of the box draw logic
        static void DrawCalenderBox(string className, (int, int) iBoxCoords)
        {

            if (className == "C#")
            {
                Console.ForegroundColor = ConsoleColor.Red;
            }
            else if (className == "DB")
            {
                Console.ForegroundColor = ConsoleColor.Green;
            }

            WriteAt("+---------------+", iBoxCoords, 0);
            WriteAt("|---------------|", iBoxCoords, 1);
            WriteAt($"|------{className}-------|", iBoxCoords, 2);
            WriteAt("|---------------|", iBoxCoords, 3);
            WriteAt("+---------------+", iBoxCoords, 4);

            Console.ResetColor();
        }

        // Writes text at given coordinates
        // can also increase row incase you are trying to print a big picture
        // (like we are using this method for)
        static void WriteAt(string sText, (int, int) coord, int yOrder)
        {
            Console.SetCursorPosition(coord.Item1, coord.Item2+yOrder);
            Console.Write(sText);
        }
    }
}