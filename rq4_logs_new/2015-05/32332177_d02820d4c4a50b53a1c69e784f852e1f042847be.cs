using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Shapes;

namespace PiffTeam
{
    /// <summary>
    /// Interaction logic for Window1.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        
        public Window1 window2=new Window1();
        public TextBox infocus;
        public double modszer;
        public string modszertext;
        public MainWindow()
        {
            InitializeComponent();
            angolnyelv();
            
        }

        private void ntTovabbgomb_Click(object sender, RoutedEventArgs e)
        {
            
            string egyenlet;
            string jobboldal;
            string baloldal;

            if ((szamlalo1.Text != "") && (szamlalo2.Text != ""))
            {

                if (nevezo1.Visibility == Visibility.Visible)
                {
                    baloldal = "(" + szamlalo1.Text + ")" + " / " + "(" + nevezo1.Text + ")";
                }
                else
                {
                    baloldal = szamlalo1.Text;
                }
                if (nevezo2.Visibility == Visibility.Visible)
                {
                    jobboldal = "(" + szamlalo2.Text + ")" + " / " + "(" + nevezo2.Text + ")";
                }
                else
                {
                    jobboldal = szamlalo2.Text;
                }

                egyenlet = baloldal + "=" + jobboldal;
                Main.Hide();
                window2.Owner = Main;
                window2.Show();
                window2.textBlock2.Text = modszertext;
                window2.textBlock1.Text = egyenlet;
            }
            else
            {
                MessageBox.Show("Túl kevés bevitt adat!");
            }
        }

        private void elsoplusz_Click(object sender, RoutedEventArgs e)
        {
            elsoplusz.Visibility = Visibility.Hidden;
            osztas.Visibility = Visibility.Visible;
            nevezo1.Visibility = Visibility.Visible;
            Egyenlosegjel.Margin = new Thickness(155, 45, 0, 0);
            nevezo1torles.Visibility = Visibility.Visible;
            if (nevezo2.Visibility == Visibility.Visible)
            {
                //Egyenlosegjel.Margin = new Thickness(155, 63, 0, 0);
                szamlalo1.Margin = new Thickness(15, 15, 0, 0);
            }
            else
                szamlalo2.Margin = new Thickness(220, 45, 0, 0);
            masodikplusz.Margin = new Thickness(275, 100, 0, 0);
        }

        private void nevezo1torles_Click(object sender, RoutedEventArgs e)
        {
            nevezo1torles.Visibility = Visibility.Hidden;
            elsoplusz.Visibility = Visibility.Visible;
            osztas.Visibility = Visibility.Hidden;
            nevezo1.Visibility = Visibility.Hidden;
            Egyenlosegjel.Margin = new Thickness(155, 70, 0, 0);
            if (nevezo2.Visibility == Visibility.Visible)
            {
                szamlalo1.Margin = new Thickness(15, 45, 0, 0);
                elsoplusz.Margin = new Thickness(70, 100, 0, 0);

            }
            else
            {
                Egyenlosegjel.Margin = new Thickness(155, 10, 0, 0);
                szamlalo2.Margin = new Thickness(220, 15, 0, 0);
                masodikplusz.Margin = new Thickness(275, 70, 0, 0);
                elsoplusz.Margin = new Thickness(70, 70, 0, 0);
            }
        }

        private void masodikplusz_Click(object sender, RoutedEventArgs e)
        {
            masodikplusz.Visibility = Visibility.Hidden;
            osztas2.Visibility = Visibility.Visible;
            nevezo2.Visibility = Visibility.Visible;
            nevezo2torles.Visibility = Visibility.Visible;
            if (nevezo1.Visibility == Visibility.Visible)
            {
                //Egyenlosegjel.Margin = new Thickness(155, 63, 0, 0);
                szamlalo2.Margin = new Thickness(220, 15, 0, 0);
                masodikplusz.Margin = new Thickness(275, 70, 0, 0);
            }
            else
            {
                Egyenlosegjel.Margin = new Thickness(155, 45, 0, 0);
                szamlalo1.Margin = new Thickness(15, 45, 0, 0);
                elsoplusz.Margin = new Thickness(70, 100, 0, 0);
            }
        }

        private void nevezo2torles_Click(object sender, RoutedEventArgs e)
        {
            nevezo2torles.Visibility = Visibility.Hidden;
            masodikplusz.Visibility = Visibility.Visible;
            osztas2.Visibility = Visibility.Hidden;
            nevezo2.Visibility = Visibility.Hidden;
            if ((nevezo1.Visibility == Visibility.Visible) && (nevezo2.Visibility == Visibility.Visible))
            {
                Egyenlosegjel.Margin = new Thickness(155, 45, 0, 0);
            }
            if (nevezo1.Visibility == Visibility.Visible)
            {
                Egyenlosegjel.Margin = new Thickness(155, 45, 0, 0);
                szamlalo2.Margin = new Thickness(220, 45, 0, 0);
                masodikplusz.Margin = new Thickness(275, 100, 0, 0);
            }
            else
            {
                Egyenlosegjel.Margin = new Thickness(155, 10, 0, 0);
                szamlalo1.Margin = new Thickness(15, 15, 0, 0);
                elsoplusz.Margin = new Thickness(70, 70, 0, 0);
                masodikplusz.Margin = new Thickness(275, 70, 0, 0);
            }
        }

        private void kilepes2_Click(object sender, RoutedEventArgs e)
        {
            System.Environment.Exit(0);
        }


        private void Window_Closing(object sender, System.ComponentModel.CancelEventArgs e)
        {
            System.Environment.Exit(0);
        }

        bool IsDigitsOnly(string str)
        {
            foreach (char c in str)
            {
                if (c < '0' || c > '9')
                    return false;
            }

            return true;
        }



        private void char1_Click(object sender, RoutedEventArgs e)
        {
            int kari;
            string symbol;
            string first;
            string second;
            symbol = sender.ToString();
            symbol = symbol.Substring(symbol.Length - 1);
            if (infocus != null)
            {
                kari = infocus.CaretIndex;
                first = infocus.Text.Substring(0, kari);
                second = infocus.Text.Substring(kari);
                infocus.Text = first + symbol + second;
                infocus.Focus();
                infocus.CaretIndex = kari+1;
            }
            
        }


        private void szamlalo1_GotFocus(object sender, RoutedEventArgs e)
        {
            infocus = szamlalo1;
        }

        private void nevezo1_GotFocus(object sender, RoutedEventArgs e)
        {
            infocus = nevezo1;
        }

        private void szamlalo2_GotFocus(object sender, RoutedEventArgs e)
        {
            infocus = szamlalo2;
        }

        private void nevezo2_GotFocus(object sender, RoutedEventArgs e)
        {
            infocus = nevezo2;
        }

        private void rb1_Checked(object sender, RoutedEventArgs e)
        {
            modszer = 1;
            modszertext = rb1.Content.ToString();
            ntTovabbgomb.IsEnabled = true;
            
        }

        private void rb2_Checked(object sender, RoutedEventArgs e)
        {
            modszer = 2;
            modszertext = rb2.Content.ToString();
            ntTovabbgomb.IsEnabled = true;
        }

        private void rb3_Checked(object sender, RoutedEventArgs e)
        {
            modszer = 3;
            modszertext = rb3.Content.ToString();
            ntTovabbgomb.IsEnabled = true;
        }

        private void rb4_Checked(object sender, RoutedEventArgs e)
        {
            modszer = 4;
            modszertext = rb4.Content.ToString();
            ntTovabbgomb.IsEnabled = true;
        }

        public void angolnyelv()
        {
            window2.Title = "Differential Equation Solver";
            Main.Title = "Differential Equation Solver";
            kilepes2.Content = "Quit";
            ntTovabbgomb.Content = "Next";
            groupBox1.Header = "Solver Method";
            groupBox2.Header = "Special Characters";
            label1.Content = "Language:";
            groupBox3.Header = "Equation submit";
            window2.groupBox1.Header = "Equation";
            window2.groupBox2.Header = "Chosen Solver Method";
            window2.groupBox3.Header = "Submit Time Values";
            window2.label1.Content = "Start Time";
            window2.label2.Content = "End Time";
            window2.label3.Content = "Interval";
            window2.kilepes.Content = "Quit";
            window2.Tovabbgomb.Content = "Next";
            window2.Back.Content = "Back";
        }

        private void magyarnyelv()
        {
            window2.Title = "Differenciál Egyenlet Megoldó";
            Main.Title = "Differenciál Egyenlet Megoldó";
            kilepes2.Content = "Kilépés";
            ntTovabbgomb.Content = "Tovább";
            groupBox1.Header = "Megoldási módszer";
            groupBox2.Header = "Különleges karakterek";
            groupBox3.Header = "Egyenlet megadása";
            label1.Content = "Nyelv:";
            window2.groupBox1.Header = "Egyenlet";
            window2.groupBox2.Header = "Választott megoldási módszer";
            window2.groupBox3.Header = "Időadatok megadása";
            window2.label1.Content = "Kezdőidőpont";
            window2.label2.Content = "Végidőpont";
            window2.label3.Content = "Lépés";
            window2.kilepes.Content = "Kilépés";
            window2.Tovabbgomb.Content = "Tovább";
            window2.Back.Content = "Vissza";
        }


        private void nemetnyelv()
        {
            window2.Title = "Differentialgleichungslöser";
            Main.Title = "Differentialgleichungslöser";
            kilepes2.Content = "Austritt";
            ntTovabbgomb.Content =  "Weiter";
            groupBox1.Header = "Gleichungstyp";
            groupBox2.Header = "Spezifische Charaktere";
            groupBox3.Header = "Gleichungübergabe";
            label1.Content = "Sprache:";
            window2.groupBox1.Header = "Gleichung";
            window2.groupBox2.Header = "Gewähltes Gleichungstyp";
            window2.groupBox3.Header = "Zeitdaten übergabe";
            window2.label1.Content = "Startzeit";
            window2.label2.Content = "Endzeit";
            window2.label3.Content = "Intervall";
            window2.kilepes.Content = "Austritt";
            window2.Tovabbgomb.Content = "Weiter";
            window2.Back.Content = "Zurück";
        }


        private void nyelvvalasztas_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            switch (nyelvvalasztas.SelectedIndex.ToString())
            {
                case "0":
                    angolnyelv();
                    break;
                case "1":
                    magyarnyelv();
                    break;
                case "2":
                    nemetnyelv();
                    break;
            }
        }

      




    }
}