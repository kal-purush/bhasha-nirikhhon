using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ConverterLib;

namespace ConsoleAPI
{
    internal class Commands
    {
        public void Dialog()
        {
            bool running = true;
            while (running)
            {
                string commandString = Console.ReadLine();
                CommandParse(commandString);
                try
                {
                switch (_command)
                {
                    case "info":
                        Info();
                        break;
                    case "clear":
                        Clear();
                        break;
                    case "exit":
                        running = false;
                        break;
                    case "help":
                        Help();
                        break;
                    default:
                        Console.WriteLine(_converter.GetConvertedValue(_command, Convert.ToDouble(args[0]),args[1],args[2]));
                        break;
                }
                }
                catch
                {
                    Console.WriteLine("Неверный ввод!");
                }
            }
        }
        void Info()
        {
            foreach (string value in _converter.GetPhysicValuesList())
            {
                Console.WriteLine(value + ":");
                foreach (string meassure in _converter.GetMeassureList(value))
                {
                    Console.WriteLine("\t" + meassure);
                }
            }
        }
        void Clear()
        {
            Console.Clear();
        }
        void Help()
        {
            string[] commands =
            {
                "info"
            };
        }
        string _command = "";
        string[] args = new string[3];
        Converter _converter = new Converter();

        void CommandParse(string commandString)
        {
            string[] str = commandString.Split(' ');
            _command = str[0];
            if (str.Length > 1)
            {
                args[0] = str[1];
                args[1] = str[2];
                args[2] = str[3];
            }
        }
        List<string> _commands = new List<string>()
        {
            "info",
            "clear",
            "exit",
            "help",
            "commands"
        };
    }
}