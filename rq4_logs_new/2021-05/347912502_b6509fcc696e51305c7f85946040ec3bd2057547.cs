using System;
using GameofLife.config;

using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Shapes;
using GameofLife.config;

namespace GameofLife
{
    /// <summary>
    /// Interaction logic for ConfigWindow.xaml
    /// </summary>
    /// 




    public partial class ConfigWindow : Window
    {

        public static int anzahlZellenBreit;

        public static int anzahlZellenHoch;

        public static Brush primColor;

        public static Brush secColor;

        public ConfigWindow()
        {
            InitializeComponent();
            zellenHoch.Text = GameofLife.config.Config.anzahlZellenHoch.ToString();
            zellenBreit.Text = GameofLife.config.Config.anzahlZellenBreit.ToString();

            ColorOne.Text = GameofLife.config.Config.primColor.ToString();
            ColorTwo.Text = GameofLife.config.Config.secColor.ToString();
        }

         private void Save_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                int height = GameofLife.config.Config.anzahlZellenHoch = int.Parse(zellenHoch.Text);
                int width = GameofLife.config.Config.anzahlZellenBreit = int.Parse(zellenBreit.Text);

                //MessageBox.Show("Erfolgreich Hoehe:" + GameofLife.config.Config.anzahlZellenHoch + "Erfolgreich Breite:" + GameofLife.config.Config.anzahlZellenBreit);

                this.Close();
               
            }
            catch
            {
                MessageBox.Show("Bitte geben Sie eine Zahl ein.", "Fehler");
            }


            
        }
    }
}