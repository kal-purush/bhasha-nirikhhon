using EventPlanner.Models;
using EventPlanner.ViewModels;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
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

namespace EventPlanner.Components
{
    /// <summary>
    /// Interaction logic for RatedOrganizerList.xaml
    /// </summary>
    public partial class RatedOrganizerList : UserControl
    {
        public RatedOrganizerList()
        {
            InitializeComponent();
        }

        private void sortByComboBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            ComboBoxItem item = (ComboBoxItem)e.AddedItems[0];
            organizersListBox.Items.SortDescriptions.Clear();
            organizersListBox.Items.SortDescriptions.Add(new SortDescription(item.Tag.ToString(),
                                                                             item.Tag.ToString() == "Rating" ?
                                                                                ListSortDirection.Descending
                                                                                : ListSortDirection.Ascending));
        }

        private void sortByComboBox_Loaded(object sender, RoutedEventArgs e)
        {
            ComboBox comboBox = (ComboBox)sender;
            comboBox.SelectedIndex = 0;
        }
    }
}