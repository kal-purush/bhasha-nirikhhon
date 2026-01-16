using ConsoleMenu.Library.Abstracts;

namespace ConsoleMenu.Library.FormInput;
public interface IFormInput : IRenderContent
{
    /// <summary>
    /// Displays a form requesting input from the user.
    /// TEXT: Text the user entered. If user pressed ESC, this is an empty string.
    /// </summary>
    /// <param name="text">Text the user entered. If user pressed ESC, this is an empty string.</param>
    /// <returns>True == Received input. (Empty string is an input) 
    /// False == No input received, most likely bacause user pressed ESC.</returns>
    bool GetUserInput(out string text);
}