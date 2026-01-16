using System.Collections.Generic;
using DNX.Helpers.Console.Interfaces;

namespace DNX.Helpers.Console.Text
{
    /// <summary>
    /// Class FormattedText.
    /// </summary>
    /// <seealso cref="DNX.Helpers.Console.Interfaces.IConsoleText" />
    public static class ConsoleTextHelper
    {
        /// <summary>
        /// Parses the specified text.
        /// </summary>
        /// <param name="text">The text.</param>
        /// <returns>IList&lt;IConsoleText&gt;.</returns>
        public static IList<IConsoleText> Parse(string text)
        {
            var list = new List<IConsoleText>();

            var parseableText = new string(text.ToCharArray());

            while (!string.IsNullOrEmpty(parseableText))
            {
                var item = ParseItem(ref parseableText);
                if (item != null)
                {
                    list.Add(item);
                }
            }

            return list;
        }

        /// <summary>
        /// Parses the item.
        /// </summary>
        /// <param name="text">The text.</param>
        /// <returns>DNX.Helpers.Console.Interfaces.IConsoleText.</returns>
        public static IConsoleText ParseItem(ref string text)
        {
            if (ColouredText.CanParse(text))
            {
                return ColouredText.Parse(ref text);
            }

            if (PlainText.CanParse(text))
            {
                return PlainText.Parse(ref text);
            }

            return null;
        }
    }
}