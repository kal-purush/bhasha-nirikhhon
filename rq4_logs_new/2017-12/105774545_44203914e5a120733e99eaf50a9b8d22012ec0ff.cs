using System;
using System.Linq;
using DNX.Helpers.Console.Enums;

namespace DNX.Helpers.Console.Text
{
    /// <summary>
    /// Class ConsoleColourAndType.
    /// </summary>
    public class ConsoleColourDefinition
    {
        /// <summary>
        /// Gets or sets the colour.
        /// </summary>
        /// <value>The colour.</value>
        public ConsoleColor Colour { get; set; }

        /// <summary>
        /// Gets or sets the type of the colour.
        /// </summary>
        /// <value>The type of the colour.</value>
        public ColorType ColourType { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="ConsoleColourDefinition"/> class.
        /// </summary>
        /// <param name="consoleColor">Color of the console.</param>
        public ConsoleColourDefinition(ConsoleColor consoleColor)
            : this(consoleColor, ColorType.Foreground)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ConsoleColourDefinition"/> class.
        /// </summary>
        /// <param name="consoleColor">Color of the console.</param>
        /// <param name="colorType">Type of the color.</param>
        public ConsoleColourDefinition(ConsoleColor consoleColor, ColorType colorType)
        {
            Colour     = consoleColor;
            ColourType = colorType;
        }

        /// <summary>
        /// To the text.
        /// </summary>
        /// <returns>System.String.</returns>
        public string ToText()
        {
            return string.Format("{0}:{1}", ColourType, Colour);
        }

        /// <summary>
        /// Froms the text.
        /// </summary>
        /// <param name="definitionText">The definition text.</param>
        /// <returns>ConsoleColourDefinition.</returns>
        /// <exception cref="System.Exception">
        /// </exception>
        public static ConsoleColourDefinition FromText(string definitionText)
        {
            var parts = definitionText
                .Split(":".ToCharArray(), StringSplitOptions.RemoveEmptyEntries);

            var colourText = parts.Last();
            var colourTypeText = parts.Length > 1
                ? parts.First()
                : ColorType.Foreground.ToString();

            ConsoleColor colour;
            ColorType colourType;

            if (!Enum.TryParse(colourText, true, out colour))
            {
                return null;
                throw new Exception(string.Format("Invalid Console Colour: {0}", colourText));
            }
            if (!Enum.TryParse(colourTypeText, true, out colourType))
            {
                return null;
                throw new Exception(string.Format("Invalid Colour Type: {0}", colourTypeText));
            }

            var instance = new ConsoleColourDefinition(colour, colourType);

            return instance;
        }
    }
}