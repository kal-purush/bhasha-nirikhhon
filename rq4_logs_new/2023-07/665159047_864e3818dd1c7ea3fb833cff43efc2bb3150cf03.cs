using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BankomatConsole
{
    internal class AsciiArt
    {
        private readonly string asciiArt;

        public AsciiArt()
        {
            asciiArt = @"
    __     __                  _______ __  __ 
    \ \   / /               /\|__   __|  \/  |
     \ \_/ /__  _   _ _ __ /  \  | |  | \  / |
      \   / _ \| | | | '__/ /\ \ | |  | |\/| |
       | | (_) | |_| | | / ____ \| |  | |  | |
       |_|\___/ \__,_|_|/_/    \_\_|  |_|  |_|
                 contact: michal@krezlewicz.pl
";
        }

        public void DisplayAscii()
        {
            Console.WriteLine(asciiArt);
        }
    }
}