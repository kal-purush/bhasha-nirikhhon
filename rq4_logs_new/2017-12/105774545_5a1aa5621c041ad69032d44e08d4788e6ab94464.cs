using System;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Linq;
using DNX.Helpers.Assemblies;
using DNX.Helpers.Console.Enums;
using DNX.Helpers.Console.Interfaces;
using DNX.Helpers.Console.Modifiers;
using DNX.Helpers.Strings.Interpolation;

namespace DNX.Helpers.Console.Extensions
{

    /// <summary>
    /// Class ConsoleExtensions.
    /// </summary>
    public static class ConsoleWriterExtensions
    {
        /// <summary>
        /// The application headers text
        /// </summary>
        public static readonly string[] ApplicationHeadersText =
        {
            "{Name} v{SimplifiedVersion} - {Description}",
            "{Copyright}"
        };

        /// <summary>
        /// Sets the next writing position coordinates
        /// </summary>
        /// <param name="textwriter">The textwriter.</param>
        /// <param name="point">The point.</param>
        public static void MoveTo(this TextWriter textwriter, Point point)
        {
            textwriter.MoveTo(point.X, point.Y);
        }

        /// <summary>
        /// Sets the next writing position coordinates
        /// </summary>
        /// <param name="textwriter">The textwriter.</param>
        /// <param name="x">The x.</param>
        /// <param name="y">The y.</param>
        public static void MoveTo(this TextWriter textwriter, int x, int y)
        {
            x = Math.Min(Math.Max(0, x), System.Console.WindowWidth);
            y = Math.Min(Math.Max(0, y), System.Console.WindowHeight);

            System.Console.CursorLeft = x;
            System.Console.CursorTop = y;
        }

        /// <summary>
        /// Displays the text at the specified position coordinates
        /// </summary>
        /// <param name="textWriter">The text writer.</param>
        /// <param name="x">The x.</param>
        /// <param name="y">The y.</param>
        /// <param name="format">The format.</param>
        /// <param name="args">The arguments.</param>
        public static void WriteAt(this TextWriter textWriter, int x, int y, string format, params object[] args)
        {
            textWriter.WriteAt(x, y, string.Format(format, args));
        }

        /// <summary>
        /// Displays the text at the specified position coordinates
        /// </summary>
        /// <param name="textWriter">The text writer.</param>
        /// <param name="x">The x.</param>
        /// <param name="y">The y.</param>
        /// <param name="text">The text.</param>
        public static void WriteAt(this TextWriter textWriter, int x, int y, string text)
        {
            textWriter.WriteAt(x, y, DisplayAtAlignment.Left, text);
        }

        /// <summary>
        /// Displays the text at the specified position coordinates
        /// </summary>
        /// <param name="textWriter">The text writer.</param>
        /// <param name="x">The x.</param>
        /// <param name="y">The y.</param>
        /// <param name="alignment">The alignment.</param>
        /// <param name="format">The format.</param>
        /// <param name="args">The arguments.</param>
        public static void WriteAt(this TextWriter textWriter, int x, int y, DisplayAtAlignment alignment, string format, params object[] args)
        {
            textWriter.WriteAt(x, y, alignment, string.Format(format, args));
        }

        /// <summary>
        /// Displays the text at the specified position coordinates
        /// </summary>
        /// <param name="textWriter">The text writer.</param>
        /// <param name="x">The x.</param>
        /// <param name="y">The y.</param>
        /// <param name="alignment">The alignment.</param>
        /// <param name="text">The text.</param>
        public static void WriteAt(this TextWriter textWriter, int x, int y, DisplayAtAlignment alignment, string text)
        {
            if (!string.IsNullOrEmpty(text))
            {
                switch (alignment)
                {
                    case DisplayAtAlignment.Right:
                        x -= text.Length;
                        break;

                    case DisplayAtAlignment.Centre:
                        x -= (text.Length / 2);
                        break;
                }
            }

            textWriter.MoveTo(x, y);

            textWriter.Write(text);
        }

        /// <summary>
        /// Writes the specified coloured text.
        /// </summary>
        /// <param name="textWriter">The text writer.</param>
        /// <param name="consoleText">The text piece.</param>
        public static void Write(this TextWriter textWriter, IConsoleText consoleText)
        {
            consoleText.Write(textWriter);
        }

        /// <summary>
        /// Writes the line.
        /// </summary>
        /// <param name="textWriter">The text writer.</param>
        /// <param name="consoleText">The text piece.</param>
        public static void WriteLine(this TextWriter textWriter, IConsoleText consoleText)
        {
            consoleText.Write(textWriter);
        }

        /// <summary>
        /// Writes the application header.
        /// </summary>
        /// <param name="textWriter">The text writer.</param>
        public static void WriteApplicationHeaders(this TextWriter textWriter)
        {
            textWriter.WriteApplicationHeaders(AssemblyDetails.ForEntryPoint());
        }

        /// <summary>
        /// Writes the application header.
        /// </summary>
        public static void WriteApplicationHeaders(this TextWriter textWriter, IAssemblyDetails assemblyDetails)
        {
            textWriter.WriteApplicationHeaders(assemblyDetails, ApplicationHeadersText);
        }

        /// <summary>
        /// Writes the application header.
        /// </summary>
        /// <param name="textWriter">The text writer.</param>
        /// <param name="assemblyDetails">The assembly details.</param>
        /// <param name="headers">The headers.</param>
        /// <remarks>
        /// Default named instance is IAssemblyDetails
        /// 'version' is IAssemblyDetails.Version
        /// 'assemblyName' is IAssemblyDetails.AssemblyName
        /// </remarks>
        public static void WriteApplicationHeaders(this TextWriter textWriter, IAssemblyDetails assemblyDetails, IList<string> headers)
        {
            var instances = new[]
            {
                new NamedInstance(assemblyDetails),
                new NamedInstance(assemblyDetails.Version, "version"),
                new NamedInstance(assemblyDetails.AssemblyName, "assemblyName")
            };

            headers
                .ToList()
                .ForEach(h => textWriter.WriteLine(h.InterpolateWithAll(instances)));
        }
    }
}