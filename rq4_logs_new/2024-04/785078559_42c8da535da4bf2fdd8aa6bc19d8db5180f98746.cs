using System;
using System.Text.RegularExpressions;

//Regex Symbols:
//{
//    -Metacharacters:
//     - `.` (dot): Matches any character except a newline.
//     - `*`: Matches zero or more occurrences of the preceding character or group.
//     - `+`: Matches one or more occurrences of the preceding character or group.
//     - `?`: Matches zero or one occurrence of the preceding character or group.
//     - `^`: Anchors the pattern to the start of the string.
//     - `$`: Anchors the pattern to the end of the string.
//   - Character Classes:
//    - `[ ]`: Matches any character within the brackets.
//     - `[^ ]`: Matches any character not within the brackets.
//     - `-`: Represents a range of characters within a character class.
//     - `\d`: Matches any digit character (0-9).
//     - `\w`: Matches any word character (alphanumeric and underscore).
//     - `\s`: Matches any whitespace character.

//   - Anchors:
//     - `\b`: Matches a word boundary.
//     - `\B`: Matches a non-word boundary.
//     - `^`: Matches the start of a line (when used with the multiline option).
//     - `$`: Matches the end of a line (when used with the multiline option).
//}


namespace RegexProblem
{
    internal class Program
    {
        static void Main(string[] args)
        {

            // create pattern 
            string pattern = @"[a-zA-Z][0-9]";
            string phonePattern = @"^[789][0-9]{9}$";
           // string emailpattern = @"[a-zA-Z0-9_\-\.]+[@][a-z]+[\.]{2,3}";
            string emailpattern = @"^\w+@\w+\.[a-zA-Z]{2,3}$";
            // take input from user
            while (true)
            {

               
            Console.WriteLine("Enter name : ");
                string name = Console.ReadLine();
                Console.WriteLine("name :" + Regex.IsMatch(name, pattern));

                Console.WriteLine("Enter phone : ");
            string phone = Console.ReadLine();
            Console.WriteLine("phone :"+Regex.IsMatch(phone,phonePattern));

                Console.WriteLine("Enter email : ");
                string mail = Console.ReadLine();
                Console.WriteLine("email :" + Regex.IsMatch(mail, emailpattern));
            }

            //phone number pattern 





             





        }
    }
}