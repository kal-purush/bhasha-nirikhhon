using System.Diagnostics.CodeAnalysis;
using System.Drawing;
using System.Windows.Forms;

namespace ExitOnX
{
    [SuppressMessage("ReSharper", "VirtualMemberCallInConstructor")]
    class ExitOnX:Form
    {
        public static void Main()
        {
            Application.Run(new ExitOnX());
        }

        public ExitOnX()
        {
            Text = "Exit on X";
            BackColor = SystemColors.Window;
            ForeColor = SystemColors.WindowText;
        }

        protected override void OnKeyDown(KeyEventArgs e)
        {
            if (e.KeyCode == Keys.X)
            {
                Close();
            }
        }
    }
}