using System;
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
using Monopoly_DAL;

namespace Project_Monopoly
{
    /// <summary>
    /// Interaction logic for KansToevoegen.xaml
    /// </summary>
    public partial class KansToevoegen : Window
    {
        public KansToevoegen()
        {
            InitializeComponent();
        }

        private void btnStop_Click(object sender, RoutedEventArgs e)
        {
            this.Close();
        }

        private void btnOpslaan_Click(object sender, RoutedEventArgs e)
        {
            string foutmeldingen = Valideer("Omschrijving");
            foutmeldingen += Valideer("Bedrag");
            foutmeldingen += Valideer("AantalPosities");

            if (string.IsNullOrWhiteSpace(foutmeldingen))
            {
                Monopoly_DAL.Kans kans = new Monopoly_DAL.Kans();
                kans.type = "Kans";
                kans.omschrijving = kansOmschrijving.Text;
                kans.bedrag = int.Parse(kansBedrag.Text);
                kans.aantalPosities = int.Parse(kansPosities.Text);
                kans.houbij = kansPosities.IsEnabled;

                int ok = DatabaseOperations.KansToevoegen(kans);

                if (ok == 0)
                {
                    MessageBox.Show("Kanskaart is niet toegevoegd");
                }
            }
            else
            {
                MessageBox.Show(foutmeldingen);
            }
        }

        private string Valideer(string columnName)
        {
            if (columnName == "Omschrijving" && kansOmschrijving.Text == null)
            {
                return "Voeg een omschrijving toe!" + Environment.NewLine; 
            }

            if (columnName == "Bedrag")
            {
                if (kansBedrag.Text == null)
                {
                    return "Voeg een bedrag toe!" + Environment.NewLine;
                }
                if (!int.TryParse(kansBedrag.Text, out int bedrag))
                {
                    return "Bedrag moet een numerieke waarde zijn!" + Environment.NewLine;
                }
                
            }

            if (columnName == "AantalPosities")
            {
                if (!int.TryParse(kansPosities.Text, out int posities))
                {
                    return "Het aantal posities moet een numerieke waarde zijn!" + Environment.NewLine;
                }
                if(posities < -10)
                {
                    return "De waarde van posities is te klein!" + Environment.NewLine;
                }
                if(posities > 10)
                {
                    return "De waarde van positeis is te groot!" + Environment.NewLine;
                }
                
            }

            return "";
        }
    }
}