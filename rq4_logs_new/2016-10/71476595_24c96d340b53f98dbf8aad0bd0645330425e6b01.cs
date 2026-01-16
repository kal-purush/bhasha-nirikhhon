using System.ComponentModel.Composition;
using System.Windows;
using System.Windows.Forms;

namespace TplPlayground.Core.Dialogs
{
    [Export(typeof(IFolderBrowserDialog))]
    [PartCreationPolicy(CreationPolicy.NonShared)]
    internal class WindowsFormsFolderBrowserDialog : IFolderBrowserDialog
    {
        private string _description;
        private string _selectedPath;

        [ImportingConstructor]
        public WindowsFormsFolderBrowserDialog()
        {
            RootFolder = System.Environment.SpecialFolder.MyComputer;
            ShowNewFolderButton = false;
        }

        #region IFolderBrowserDialog Members

        public string Description
        {
            get { return _description ?? string.Empty; }
            set { _description = value; }
        }

        public System.Environment.SpecialFolder RootFolder
        {
            get;
            set;
        }

        public string SelectedPath
        {
            get { return _selectedPath ?? string.Empty; }
            set { _selectedPath = value; }
        }

        public bool ShowNewFolderButton
        {
            get;
            set;
        }

        public bool? ShowDialog()
        {
            using (var dialog = CreateDialog())
            {
                var result = dialog.ShowDialog() == DialogResult.OK;
                if (result)
                {
                    SelectedPath = dialog.SelectedPath;
                }

                return result;
            }
        }

        public bool? ShowDialog(Window owner)
        {
            using (var dialog = CreateDialog())
            {
                var result = dialog.ShowDialog(owner.AsWin32Window()) == DialogResult.OK;
                if (result)
                {
                    SelectedPath = dialog.SelectedPath;
                }

                return result;
            }
        }

        #endregion

        private FolderBrowserDialog CreateDialog()
        {
            return new FolderBrowserDialog
            {
                Description = Description,
                RootFolder = RootFolder,
                SelectedPath = SelectedPath,
                ShowNewFolderButton = ShowNewFolderButton
            };
        }
    }
}