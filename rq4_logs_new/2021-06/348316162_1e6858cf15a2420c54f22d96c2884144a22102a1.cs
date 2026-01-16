using EventPlanner.Models;
using EventPlanner.ViewModels;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
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

namespace EventPlanner.Windows.User
{
    /// <summary>
    /// Interaction logic for MakeRequestPage.xaml
    /// </summary>
    public partial class MakeRequestWindow : Window
    {
        public MakeRequestWindow()
        {
            InitializeComponent();
        }

        private void organizerSearchTextBox_KeyDown(object sender, KeyEventArgs e)
        {
            if (e.Key == Key.Enter)
            {
                OrganizersViewModel viewModel = (OrganizersViewModel)ratedOrganizerList.DataContext;
                if (viewModel != null)
                {
                    viewModel.FilterOrganizersCmd.Execute(organizerSearchTextBox.Text);
                }
            }
        }
    }
}