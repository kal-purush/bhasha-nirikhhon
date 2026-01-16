using System.Text;

namespace Identifiers.Czech
{
    /// <summary>
    /// A pattern of a string, used to transform a value from and to the strin with a certain pattern..
    /// </summary>
    /// <typeparam name="T">Type of value.</typeparam>
    /// <see cref="IFormattable"/>.
    public interface IPattern<T>
    {
        /// <summary>
        /// Format the <paramref name="value"/> according to the pattern.
        /// </summary>
        /// <param name="value">Value to format.</param>
        /// <returns>Formatted string.</returns>
        string Format(T value);

        /// <summary>
        /// Parse a text into the value. It doesn't thow exceptions, if an error 
        /// happens during parsing, return unsuccessfull <see cref="ParseResult{T}"/>.
        /// </summary>
        /// <param name="text">Text to parse.</param>
        /// <returns>Parsing result.</returns>
        /// <exception cref="ArgumentNotNull">If <paramref name="text"/> is <c>null</c>.</exception>
        ParseResult<T> Parse(string text);
    }
}