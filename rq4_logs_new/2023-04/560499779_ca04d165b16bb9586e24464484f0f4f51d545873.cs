using Autodesk.Revit.UI;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Windows;
using System.Windows.Forms;

namespace PanelPlacement
{
    public partial class UserInterfaceDuplicatesParams : Window
    {
        public UserInterfaceDuplicatesParams(IList<string> paramsList)
        {
            InitializeComponent();
            TextBlockParams.Text = string.Join(Environment.NewLine, paramsList);
        }

        private void ButtonSave(Object sender, EventArgs e)
        {
            string assemplyPath = Assembly.GetExecutingAssembly().Location;
            string path = Path.GetDirectoryName(assemplyPath) + @"\PanelPlacementCompareParams.txt";
            FileStream file = new FileStream(path, FileMode.Create);
            StreamWriter writeFile = new StreamWriter(file);

            string[] lines = TextBlockParams.Text.Split(new string[] { "\r\n" }, StringSplitOptions.RemoveEmptyEntries);
            foreach (string line in lines)
            {
                writeFile.WriteLine(line);
            }
            writeFile.Close();

            DialogResult = true;
            Close();
        }

        private void ButtonCancel(Object sender, EventArgs e)
        {
            DialogResult = true;
            Close();
        }
    }
}