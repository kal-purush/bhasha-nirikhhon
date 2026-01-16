using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClassLibrary
{
    public class Caretaker
    {
        private Stack<Memento> history = new Stack<Memento>();

        public void SaveState(TextDocument document)
        {
            history.Push(document.CreateMemento());
        }

        public void RestoreState(TextDocument document)
        {
            if (history.Count > 0)
            {
                document.RestoreMemento(history.Pop());
            }
            else
            {
                Console.WriteLine("No states to restore");
            }
        }
    }
}