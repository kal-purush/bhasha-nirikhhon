using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClassLibrary
{
    public class LightElementNode : LightNode
    {
        public string TagName { get; set; }
        public bool IsBlock { get; set; }
        public bool IsSelfClosing { get; set; }
        public List<string> Classes { get; set; }
        public List<LightNode> Children { get; set; }

        public LightElementNode(string tagName, bool isBlock, bool isSelfClosing)
        {
            TagName = tagName;
            IsBlock = isBlock;
            IsSelfClosing = isSelfClosing;
            Classes = new List<string>();
            Children = new List<LightNode>();
        }

        public void AddChild(LightNode child)
        {
            Children.Add(child);
        }
        public void AddClass(string className)
        {
            Classes.Add(className);
        }

        public string InnerHTML()
        {
            string html = "";
            foreach (LightNode child in Children)
            {
                html += child.TemplateMethod();
            }
            return html;
        }

        public override string ToHtml()
        {
            string tagClass = Classes.Count > 0 ? $" class=\"{string.Join(" ", Classes)}\"" : "";
            string html = $"<{TagName}{tagClass}>";

            if (!IsSelfClosing)
            {
                html += InnerHTML();
                html += $"</{TagName}>";
            }

            return html;
        }

        public string OuterHTML()
        {
            return ToHtml();
        }

        protected override void OnCreated()
        {
            Console.WriteLine("LightElementNode is Created");
        }
        protected override void OnInserted()
        {
            Console.WriteLine("LightElementNode is Inserted");
        }
        protected override void OnStylesApplied()
        {
            Console.WriteLine("LightElementNode is StylesApplied");
        }
        protected override void OnClassListApplied()
        {
            Console.WriteLine("LightElementNode is ClassListApplied");
        }
        protected override void OnTextRendered()
        {
            Console.WriteLine("Text is TextRendered");
        }
        protected override void OnRemoved()
        {
            Console.WriteLine("LightElementNode is Removed");
        }
    }
}