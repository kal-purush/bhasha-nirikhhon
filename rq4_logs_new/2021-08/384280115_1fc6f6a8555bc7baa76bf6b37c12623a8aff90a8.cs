using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PasswordGenerator
{
    public class Generator
    {
        private string softwareName;

        public Generator()
        {
            this.softwareName = "";
        }

        public string GeneratePassword(TypeOfGeneration typeOfGeneration)
        {
            switch (typeOfGeneration)
            {
                case TypeOfGeneration.Simple:
                    return this.GenerateSimplePassword();
                case TypeOfGeneration.Complex:
                    return this.GenerateComplexPassword();
                default:
                    break;
            }
            throw new Exception("No password has been generated.");
        }

        private string GenerateSimplePassword()
        {
            string thirdLetter = "";
            if (this.softwareName.Length >= 4)
                thirdLetter = this.softwareName[3].ToString().ToUpper();
            else
                thirdLetter = "X";
            string generatedPassword = Properties.Settings.Default.firstpartofpwd + TransferLettersToNumbers(this.softwareName.Substring(0, 3));
            generatedPassword += thirdLetter + Properties.Settings.Default.otherchar;
            return generatedPassword;
        }

        private string GenerateComplexPassword()
        {
            string preSeed = this.TransferLettersToNumbers(this.softwareName);
            if (preSeed.Length > 7)
            {
                preSeed = preSeed.Substring(0, 7);
            }
            int seed = Convert.ToInt32(preSeed);
            Random random = new Random(seed);
            int numberOfNumbers = random.Next(1, 6);
            string numbers = "";
            for (int i = 0; i < numberOfNumbers; i++)
            {
                numbers += Convert.ToString(random.Next(0, 101));
            }
            string generatedPassword = Properties.Settings.Default.firstpartofpwd + numbers;
            string upperLetter = this.softwareName[random.Next(0, this.softwareName.Length)].ToString().ToUpper();
            generatedPassword += upperLetter + Properties.Settings.Default.otherchar;
            return generatedPassword;
        }

        private string TransferLettersToNumbers(string letters)
        {
            string numbers = "";
            foreach (char letter in letters.ToUpper())
            {
                switch (letter)
                {
                    case 'A':
                        numbers += "1";
                        break;
                    case 'B':
                        numbers += "2";
                        break;
                    case 'C':
                        numbers += "3";
                        break;
                    case 'D':
                        numbers += "4";
                        break;
                    case 'E':
                        numbers += "5";
                        break;
                    case 'F':
                        numbers += "6";
                        break;
                    case 'G':
                        numbers += "7";
                        break;
                    case 'H':
                        numbers += "8";
                        break;
                    case 'I':
                        numbers += "9";
                        break;
                    case 'J':
                        numbers += "10";
                        break;
                    case 'K':
                        numbers += "11";
                        break;
                    case 'L':
                        numbers += "12";
                        break;
                    case 'M':
                        numbers += "13";
                        break;
                    case 'N':
                        numbers += "14";
                        break;
                    case 'O':
                        numbers += "15";
                        break;
                    case 'P':
                        numbers += "16";
                        break;
                    case 'Q':
                        numbers += "17";
                        break;
                    case 'R':
                        numbers += "18";
                        break;
                    case 'S':
                        numbers += "19";
                        break;
                    case 'T':
                        numbers += "20";
                        break;
                    case 'U':
                        numbers += "21";
                        break;
                    case 'V':
                        numbers += "22";
                        break;
                    case 'W':
                        numbers += "23";
                        break;
                    case 'X':
                        numbers += "24";
                        break;
                    case 'Y':
                        numbers += "25";
                        break;
                    case 'Z':
                        numbers += "26";
                        break;
                }
            }
            return numbers;
        }

        public string SoftwareName
        {
            get { return this.softwareName; }
            set { this.softwareName = value; }
        }
    }

    public enum TypeOfGeneration
    {
        Simple,
        Complex
    }
}