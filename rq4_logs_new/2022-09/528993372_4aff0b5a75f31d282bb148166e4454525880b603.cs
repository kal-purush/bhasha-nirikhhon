using System;
using System.Collections.Generic;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Shapes;
using WpfKwiaty.Models;

namespace WpfKwiaty
{
    /// <summary>
    /// Logika interakcji dla klasy EditPlant.xaml
    /// </summary>
    public partial class EditPlant : Window
    {
        public Plant editPlant { get; set; }

        public EditPlant(Plant plant) 
        {
            InitializeComponent();
            this.editPlant = plant;

        }

        private void Button_Click(object sender, RoutedEventArgs e)
        {
            Close();
        }
    }
}