using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using MailSender.lib.Entities;

namespace MailSender
{
    /// <summary>
    /// Логика взаимодействия для SenderEditor.xaml
    /// </summary>
    public partial class SenderEditor
    {
        private WpfMailSender _WpfMailSender;

        public string NameValue { get => NameEditor.Text; set => NameEditor.Text = value; }
        public string AddressValue { get => AddressEditor.Text; set => AddressEditor.Text = value; }

        public SenderEditor(Sender Sender, WpfMailSender MainWindow)
        {
            InitializeComponent();
            NameValue = Sender.Name;
            AddressValue = Sender.Address;
            _WpfMailSender = MainWindow;
        }

        private void OnOkButtonClick(object Sender, RoutedEventArgs E)
        {
            DialogResult = true;
            Close();
        }

        private void OnCancelButtonClick(object Sender, RoutedEventArgs E)
        {
            DialogResult = false;
            Close();
        }

        private void OnCloseMainWindowClick(object Sender, RoutedEventArgs E)
        {
            _WpfMailSender.Title += "Hello World!";
        }

    }
}