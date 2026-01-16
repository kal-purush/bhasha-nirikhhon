namespace MvvX.Plugins.FileChooser
{
    public class OpenFolderResult
    {
        /// <summary>
        /// If the user clicks the OK button of the dialog that is displayed, true is returned; otherwise, false.
        /// </summary>
        public bool? Result { get; set; }

        /// <summary>
        /// Gets the path selected by the user.
        /// </summary>
        public string SelectedPath { get; set; }
    }
}