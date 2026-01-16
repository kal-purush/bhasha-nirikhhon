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

namespace ctrl_doc
{
    /// <summary>
    /// Lógica de interacción para cancelarDoc.xaml
    /// </summary>
    public partial class cancelarDoc : Window
    {
        clsDB db = new clsDB();
        static int idcan = 0;
        public cancelarDoc(int id, string dok)
        {
            InitializeComponent();
            lblttl.Content = dok;
            idcan = id;
        }

        private void btnvolv_Click(object sender, RoutedEventArgs e)
        {
            this.Close();
        }

        private void btnok_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                MessageBoxResult mbr = MessageBox.Show("Confirmar la cancelacion del documento","", MessageBoxButton.YesNo);
                if (mbr == MessageBoxResult.Yes)
                {
                    string cmd = string.Format("regCan {0}, '{1}', '{2}'", idcan, txtCom.Text, txtAut.Text);
                    db.write(cmd);
                    MessageBox.Show("El documento ha sido cancelado");
                }
                else
                {
                    MessageBox.Show("La operacion ha sido cancelada");
                }
            }
            catch (Exception z)
            {
                MessageBox.Show(z.Message);
            }
        }
    }
}