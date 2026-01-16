using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Windows.Media;
using System.Windows.Documents;

namespace WPFCommandPrompt
{
    /// <summary>
    /// Delegate for the write Event Handler.
    /// </summary>
    /// <param name="sender">The sender.</param>
    /// <param name="e">The <see cref="WPFCommandPrompt.ConsoleWriteLineEventArgs"/> instance containing the event data.</param>
    public delegate void WriteLineEventHandler(object sender, ConsoleWriteLineEventArgs e);

    /// <summary>
    /// Event arguments for a console write event.
    /// </summary>
    public class ConsoleWriteLineEventArgs : EventArgs
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ConsoleWriteLineEventArgs"/> class.
        /// </summary>
        /// <param name="message">Message to send to the console.</param>
        public ConsoleWriteLineEventArgs(string message)
        {
            this.Message = message;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ConsoleWriteLineEventArgs"/> class.
        /// </summary>
        /// <param name="message">>Message to send to the console.</param>
        /// <param name="foreground">Text color (overrides default).</param>
        public ConsoleWriteLineEventArgs(string message, Brush foreground)
        {
            this.Message = message;
            this.Foreground = foreground;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ConsoleWriteLineEventArgs"/> class.
        /// </summary>
        /// <param name="paragraph">The paragraph to write to the console.</param>
        public ConsoleWriteLineEventArgs(Paragraph paragraph)
        {
            this.Paragraph = paragraph;
        }

        /// <summary>
        /// The flow document paragraph to write to the console.
        /// </summary>
        public readonly Paragraph Paragraph;

        /// <summary>
        /// Text color.
        /// </summary>
        public readonly Brush Foreground;

        /// <summary>
        /// Message string to/from the console.
        /// </summary>
        public readonly string Message;
    }

    /// <summary>
    /// Delegate for the read Event Handler.
    /// </summary>
    /// <param name="sender">The sender.</param>
    /// <param name="e">The <see cref="WPFCommandPrompt.ConsoleReadLineEventArgs"/> instance containing the event data.</param>
    public delegate void ReadLineEventHandler(object sender, ConsoleReadLineEventArgs e);

    /// <summary>
    /// Event arguments for a console read event as a parsed array of
    /// commands or a single unparsed string of commands.
    /// </summary>
    public class ConsoleReadLineEventArgs : EventArgs
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ConsoleReadLineEventArgs"/> class.
        /// </summary>
        /// <param name="commands">The parsed commands as an array.</param>
        public ConsoleReadLineEventArgs(string[] commands)
        {
            this.Commands = commands;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ConsoleReadLineEventArgs"/> class.
        /// </summary>
        /// <param name="commandString">The un-parsed command string.</param>
        public ConsoleReadLineEventArgs(string commandString)
        {
            this.CommandString = commandString;
        }

        /// <summary>
        /// Parsed string array of commands
        /// </summary>
        public readonly string[] Commands;

        /// <summary>
        /// Un-parsed command string.
        /// </summary>
        public readonly string CommandString;
    }
}