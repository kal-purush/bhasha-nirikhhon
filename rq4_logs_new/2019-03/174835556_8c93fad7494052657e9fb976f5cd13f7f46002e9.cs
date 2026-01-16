using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp1
{
    class Letter
    {
        char letter;
        public void SetLetter(char letter)
        {
            this.letter = letter;
        }
        public char GetLetter()
        {
            return letter;
        }
        public bool IsVocalic(char letter)
        {
            this.letter = letter;
            if(letter == 'а' ||
                letter == 'о' ||
                letter == 'у' ||
                letter == 'ы' ||
                letter == 'э' ||
                letter == 'я' ||
                letter == 'ё' ||
                letter == 'ю' ||
                letter == 'и' ||
                letter == 'е')
            {
                return true;
            }
            return false;
        }
    }
}