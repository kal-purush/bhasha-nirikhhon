using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClassLibrary
{
    public class CodeHTML
    {
        private ElementsTypeFactory Factory;
        public CodeHTML()
        {
            Factory = new ElementsTypeFactory();
        }

        public LightElementNode AddHtmlTagsToLine(string line, bool isFirstLine)
        {
            LightElementNode node;

            if (isFirstLine)
            {
                node = new LightElementNode(Factory.GetElementType("h1"));
            }
            else if (line.Length < 20)
            {
                node = new LightElementNode(Factory.GetElementType("h2"));
            }
            else if (line.StartsWith(" "))
            {
                node = new LightElementNode(Factory.GetElementType("blockquote"));
            }
            else
            {
                node = new LightElementNode(Factory.GetElementType("p"));
            }

            node.AddChild(new LightTextNode(line));
            return node;
        }
    }
}