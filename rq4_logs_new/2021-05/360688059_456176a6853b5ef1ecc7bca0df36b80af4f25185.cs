using System;
using System.Windows;
using System.Windows.Controls;

namespace Projet_Mines_Official
{
    class ElementsLooper
    {
        /// <summary>
        /// this function will check if the Element is A Frameworkelement
        /// </summary>
        /// <param name="element"></param>
        /// <returns>Returns true if the element is FrameworkElement and false if it's not</returns>
        internal bool TryParseFrameworkElement(dynamic element)
        {
            try
            {
                FrameworkElement t = ((FrameworkElement)element);
            }
            catch
            {
                return false;
            }
            return true;
        }
        /// <summary>
        /// this function will check it the Passed Element Has Childs
        /// </summary>
        /// <param name="element"></param>
        /// <returns>Returns True if The Element has childs and false if it hasn't</returns>
        internal bool HasChilds(FrameworkElement element)
        {
            foreach (var e in LogicalTreeHelper.GetChildren(element))
            {
                return true;
            }
            return false;
        }
        /// <summary>
        /// this fucnction Will Loop trough all the TextBoxes inside the given Element 
        /// </summary>
        /// <param name="ParentElement"></param>
        /// <param name="typeOfElement">The type of the Element to Get</param>
        /// <param name="ExecuteInTextBox">This function will be executed for each Textbox found and Will Receive the current TextBox Found as a parameter</param>
        /// <Note PayAttention="this function may block your program (Call it in asynchronously)"></Note>
        internal void GetElements(FrameworkElement ParentElement,Type typeOfElement, Action<dynamic> ExecuteInTextBox)
        {
            foreach (var ChildElement in LogicalTreeHelper.GetChildren(ParentElement))
            {
                if (!TryParseFrameworkElement(ChildElement))
                    continue;
                if (ChildElement.GetType()==typeOfElement)
                    ExecuteInTextBox(ChildElement);
                if (HasChilds((FrameworkElement)ChildElement))
                {
                    GetElements((FrameworkElement)ChildElement,typeOfElement, ExecuteInTextBox);
                }
            }
        }
    }
}