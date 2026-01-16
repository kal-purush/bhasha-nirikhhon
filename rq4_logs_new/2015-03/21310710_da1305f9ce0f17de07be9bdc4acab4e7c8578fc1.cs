using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;
using Microsoft.Win32;

namespace Utils.WPF
{
    /// <summary>
    /// Logique d'interaction pour FilePicker.xaml
    /// </summary>
    public partial class FilePicker : UserControl, INotifyPropertyChanged
    {
        private String _filePath;
        private Object _buttonContent;

        public Object ButtonContent
        {
            get { return _buttonContent; }
            set
            {
                _buttonContent = value;
                OnPropertyChanged();
            }
        }

        public String FilePath
        {
            get { return _filePath; }
            set
            {
                _filePath = value;
                OnPropertyChanged();
            }
        }

        public FilePicker()
        {
            InitializeComponent();
        }

        private void BrowseKeyButton_Click(object sender, RoutedEventArgs e)
        {
            var ofd = GetOpenFileDialog("Ouvrir");//TODO : Localize
            ofd.ShowDialog();
            FilePath = ofd.FileName;
        }

        public static OpenFileDialog GetOpenFileDialog(String title, Boolean multipleFiles = false)
        {
            return new OpenFileDialog()
            {
                CheckFileExists = true,
                CheckPathExists = true,
                DereferenceLinks = true,
                Multiselect = multipleFiles,
                RestoreDirectory = true,
                Title = title
            };
        }

        #region Default Implementation of INotifyPropertyChanged

        public event PropertyChangedEventHandler PropertyChanged;

        private void OnPropertyChanged([CallerMemberName] string propertyName = null)
        {
            var handler = PropertyChanged;
            if (handler != null) handler(this, new PropertyChangedEventArgs(propertyName));
        }


        #endregion
    }
}