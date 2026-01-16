using HtmlAgilityPack;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace HtmlTree
{
    public class HtmlElement
    {
        public string Id { get; set; }
       
        public string Name { get; set; }
        
        public List<string> Attributes { get; set; }
        
        public List<string> Classes { get; set; }
        
        public string InnerHtml { get; set; }
        
        public HtmlElement Parent { get; set; }
        
        public List<HtmlElement> Children { get; set; }

        public HtmlElement()
        {
            Attributes = new List<string>();
            Classes = new List<string>();
            Children = new List<HtmlElement>();
        }

        public HtmlElement(string name, HtmlElement parent = null)
        {
            Name = name;
            Parent = parent;
            Attributes = new List<string>();
            Classes = new List<string>();
            Children = new List<HtmlElement>();
        }

        public IEnumerable<HtmlElement> Descendants()
        {
            Queue<HtmlElement> queue = new Queue<HtmlElement>();
            HashSet<HtmlElement> visitedElements = new HashSet<HtmlElement>();

            queue.Enqueue(this);

            while (queue.Count > 0)
            {
                var element = queue.Dequeue();
                if (visitedElements.Add(element))
                {
                    yield return element;

                    foreach (var child in element.Children)
                    {
                        queue.Enqueue(child);
                    }
                }
            }
        }

        public IEnumerable<HtmlElement> Ancestors()
        {
            var parent = this.Parent;

            while (parent != null)
            {
                yield return parent;
                parent = parent.Parent;
            }
        }

        public static List<HtmlElement> FindElements(HtmlElement element, Func<HtmlElement, bool> predicate)
        {
            List<HtmlElement> result = new List<HtmlElement>();
            HashSet<HtmlElement> visitedElements = new HashSet<HtmlElement>();
            FindElementsRecursive(element, predicate, result, visitedElements);
            return result;
        }

        private static void FindElementsRecursive(HtmlElement element, Func<HtmlElement, bool> predicate, List<HtmlElement> result, HashSet<HtmlElement> visitedElements)
        {
            if (visitedElements.Add(element) && predicate(element))
            {
                result.Add(element);
            }

            var descendants = element.Descendants();

            foreach (var descendant in descendants)
            {
                if (visitedElements.Add(descendant) && predicate(descendant))
                {
                    result.Add(descendant);
                }
            }
        }
    }
}