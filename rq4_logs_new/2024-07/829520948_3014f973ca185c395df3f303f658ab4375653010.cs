using System.Xml.Linq;

namespace SystemFinder.Shared
{
    /// <summary>
    ///     Extension methods for <see cref="XElement" />
    /// </summary>
    /// <remarks>See: https://stackoverflow.com/questions/451950/get-the-xpath-to-an-xelement</remarks>
    public static class XElementExtensions
    {
        /// <summary>
        /// Get the absolute XPath to a given XElement
        /// (e.g. "/people/person[6]/name[1]/last[1]").
        /// </summary>
        public static string GetAbsoluteXPath(this XElement element)
        {
            if (element == null)
            {
                throw new ArgumentNullException("element");
            }

            Func<XElement, string> relativeXPath = e =>
            {
                int index = e.IndexPosition();
                string name = e.Name.LocalName;

                // If the element is the root, no index is required

                var path = string.Empty;
                if (index == -1)
                {
                    path = "/" + name;
                }
                else if (e.Parent?.Elements(name).Count() < 2)
                {
                    path = "/" + name;
                }
                else
                {
                    path = $"/{name}[{index.ToString()}]";
                }

                return path;
            };

            var ancestors = from e in element.Ancestors()
                            select relativeXPath(e);

            return string.Concat(ancestors.Reverse().ToArray()) +
                   relativeXPath(element);
        }

        /// <summary>
        /// Get the index of the given XElement relative to its
        /// siblings with identical names. If the given element is
        /// the root, -1 is returned.
        /// </summary>
        /// <param name="element">
        /// The element to get the index of.
        /// </param>
        public static int IndexPosition(this XElement element)
        {
            if (element == null)
            {
                throw new ArgumentNullException("element");
            }

            if (element.Parent == null)
            {
                return -1;
            }

            int i = 1; // Indexes for nodes start at 1, not 0

            foreach (var sibling in element.Parent.Elements(element.Name))
            {
                if (sibling == element)
                {
                    return i;
                }

                i++;
            }

            throw new InvalidOperationException("element has been removed from its parent.");
        }
    }
}