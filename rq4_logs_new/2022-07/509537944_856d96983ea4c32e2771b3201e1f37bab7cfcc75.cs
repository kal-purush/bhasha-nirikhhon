using static System.Console;

namespace WordClassLibrary
{
    public class Console:IConsole
    {
        public string GetIntput()
        {
           return ReadLine();
        }

        public void WriteInput(string data)
        {
            WriteInput(data);
        }

        public void ClearScreen()
        {
            Clear();
        }
    }
}