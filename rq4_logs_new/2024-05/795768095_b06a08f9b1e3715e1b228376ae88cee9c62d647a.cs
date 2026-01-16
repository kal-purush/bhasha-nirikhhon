using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClassLibrary
{
    public class HtmlVisitor : IVisitor
    {
        public void VisitLightTextNode(LightTextNode textNode)
        {
            Console.WriteLine($"Visiting text node with text: {textNode.Text}");
        }

        public void VisitLightElementNode(LightElementNode elementNode)
        {
            Console.WriteLine($"Visiting element node with tag: {elementNode.TagName}");
        }
    }
}