using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace simplified
{
    /// <summary>
    /// class Program
    /// main class in program
    /// </summary>
    class Program
    {
        static char Start = 'P';
        static char End = 'E';
        static List<char> sequence = new List<char>();
        static bool refusal = false;

        /// <summary>
        /// method Main entry program
        /// </summary>
        /// <param name="args"></param>
        static void Main(string[] args)
        {
            inputSequence();
            if (refusal == true || sequence.Count == 0)
                Console.WriteLine("\n NACK");
            else
            {
                parseCommand();
            }
        }

        /// <summary>
        /// method inputSequence
        /// wait for begging to write command 
        /// </summary>
        static void inputSequence()
        {
            char? readChar;            

            while (true)
            {

                readChar = GetChar();
                if (!readChar.HasValue)
                    break;      

                if (readChar.Value == Start)
                {
                    sequence.Add(readChar.Value);
                    InputCommand();
                    break;
                }               

            }
        }

        /// <summary>
        /// method InputCommand
        /// input character in sequence
        /// </summary>
        static void InputCommand()
        {
            Char? readChar;

            while (true)
            {
                readChar = GetChar();
                if(!readChar.HasValue)
                {
                    sequence.Clear();
                    return;
                }
                    
                sequence.Add(readChar.Value);
                if (readChar.Value == End)
                    break;       
               
            }
        }

        /// <summary>
        /// method GetChar
        /// Check value from stream
        /// </summary>
        /// <returns>char? </returns>
        static char? GetChar()
        {
            char? readChar;

            try
            {
                readChar = Console.ReadKey().KeyChar;
                if (readChar < 32 || readChar > 127)
                    throw new InvalidOperationException();
            }
            catch (InvalidOperationException)
            {
                refusal = true;
                return null;
            }

            return readChar;
        }

        /// <summary>
        /// method parseCommand
        /// check correct command and do command
        /// </summary>
        static void parseCommand()
        {
            int indexFirst, indexLast;
            //check indexes container with character :
            try
            {
                indexFirst = sequence.FindIndex(x => x == 58);
                indexLast = sequence.FindLastIndex(x => x == 58);

                if (indexFirst == -1 || indexLast == -1 || indexFirst == indexLast)
                    throw new ArgumentNullException();

                if ((sequence.Count - indexLast - 1) != 1
               || indexFirst != 2)//check whether the last character E and the first command
                {
                    throw new ArgumentNullException();
                }
            }
            catch (ArgumentNullException)
            {
                Console.WriteLine("\n NACK");
                return;
            }
            
            switch (sequence[1])
            {
                case 'T':                    
                    outputText(indexFirst, indexLast);                    
                    break;
                case 'S':
                    makeBeep(indexFirst, indexLast);
                    break;
                default:
                    Console.WriteLine("\n NACK");
                    break;
            }
        }

        /// <summary>
        /// method outputText
        /// output param in command
        /// </summary>
        /// <param name="indexFirst"></param>
        /// <param name="indexLast"></param>
        static void outputText(int indexFirst, int indexLast)
        {
            string temp = "";
            try
            {
                temp = new string(sequence.ToArray(), indexFirst+1, indexLast- indexFirst-1);
            }
            catch(Exception)
            {
                Console.WriteLine("\n NACK");
                return;
            }

            Console.WriteLine("\n ACK");
            Console.WriteLine(temp);

        }

        /// <summary>
        /// method makeBeep
        /// Make beep if param is correct
        /// </summary>
        /// <param name="indexFirst"></param>
        /// <param name="indexLast"></param>
        static void makeBeep(int indexFirst, int indexLast)
        {
            int indexComma, frequency = 0, time= 0;
            string cfrequency = "";
            string ctime = "";
            try
            {
                indexComma = sequence.FindIndex(x => x == ',');
                if (indexComma == -1 || indexLast < indexComma || indexFirst > indexComma)
                    throw new ArgumentNullException();
            }
            catch (ArgumentNullException)
            {
                Console.WriteLine("\n NACK");
                return;
            }

            try
            {
                char[] tempArray = sequence.ToArray();
                cfrequency = new string(tempArray, indexFirst+1, indexComma - indexFirst -1);
                ctime = new string(tempArray, indexComma + 1, indexLast - indexComma-1);
                frequency = Int32.Parse(cfrequency);
                time = Int32.Parse(ctime);
            }
            catch(Exception)
            {
                Console.WriteLine("\n NACK");
                return;
            }

            Console.WriteLine("\n ACK");
            Console.Beep(frequency, time);
      

        }

    }
}