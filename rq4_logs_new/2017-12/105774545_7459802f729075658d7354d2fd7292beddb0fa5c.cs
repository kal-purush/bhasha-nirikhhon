using CommandLine;
using DNX.Helpers.Console.CommandLine.Templating;
using DNX.Helpers.Console.CommandLine.Templating.DotLiquid;

namespace DNX.Helpers.Console.CommandLine.Settings
{
    /// <summary>
    /// Class ExtendedParserSettings.
    /// </summary>
    /// <seealso cref="ParserSettings" />
    public class ExtendedParserSettings : ParserSettings
    {
        /// <summary>
        /// Gets or sets the default template engine.
        /// </summary>
        /// <value>The default template engine.</value>
        public static ITemplateEngine DefaultTemplateEngine { get; set; }

        static ExtendedParserSettings()
        {
            DefaultTemplateEngine = new DotLiquidTemplateEngine();
        }

        /// <summary>
        /// Gets or sets a value indicating whether the Parser should throw an Exception if parsing fails
        /// </summary>
        /// <value><c>true</c> to throw on parse failure; otherwise, <c>false</c>.</value>
        public bool ThrowOnParseFailure { get; set; }

        /// <summary>
        /// Gets or sets the type of the default template engine.
        /// </summary>
        /// <value>The type of the default template engine.</value>
        public ITemplateEngine TemplateEngine { get; set; }

        /// <summary>
        /// Gets or sets the width of the help text.
        /// </summary>
        /// <value>The width of the help text.</value>
        public int? HelpTextWidth { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="ParserExtendedSettings"/> class.
        /// </summary>
        public ExtendedParserSettings()
        {
            Reset();
        }

        /// <summary>
        /// Resets the settings.
        /// </summary>
        public void Reset()
        {
            ThrowOnParseFailure = true;
            TemplateEngine      = DefaultTemplateEngine;
            HelpTextWidth       = ConsoleHelper.GetConsoleWidth();
        }
    }
}