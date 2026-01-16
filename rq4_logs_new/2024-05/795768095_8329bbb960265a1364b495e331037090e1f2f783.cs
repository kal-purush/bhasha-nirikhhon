using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClassLibrary
{
    public class AddClassCommand : ICommand
    {
        private LightElementNode Element;
        private string ClassName;

        public AddClassCommand(LightElementNode element, string className)
        {
            Element = element;
            ClassName = className;
        }

        public void Execute()
        {
            Element.AddClass(ClassName);
        }

        public void Undo()
        {
            Element.Classes.Remove(ClassName);
        }
    }
}