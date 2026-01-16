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
using System.Windows.Navigation;
using System.Windows.Shapes;
using MantenedoresCRUD.modelo;
using MantenedoresCRUD.negocio;
using MantenedoresCRUD.vista;

namespace MantenedoresCRUD
{
    /// <summary>
    /// Lógica de interacción para MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {

        private Modulo_Administrador administrador;
        private Modulo_Operador operador;
        private Modulo_Piloto piloto;
        private Modulo_Consultor consultor;
        public MainWindow()
        {
            InitializeComponent();
        }

        private void DockPanel_MouseLeftButtonDown(object sender, MouseButtonEventArgs e)
        {
            this.DragMove();
        }

        private void button_Click(object sender, RoutedEventArgs e)
        {
            this.Close();
        }

        private void textBox_TextChanged(object sender, TextChangedEventArgs e)
        {

        }

        private void buttonIngreso_Click(object sender, RoutedEventArgs e)
        {
            NeUsuario login = new NeUsuario();
            Usuario usuario = new Usuario();
            int resp = new int();

            usuario.User = textBoxUsuario.Text;
            usuario.Password = passwordBox.Password;
            resp = login.Login(usuario);
            switch (resp)
            {
                case -1:
                    MessageBox.Show("Las credenciales proporcionadas son incorrectas.", "Error en el ingreso");
                    textBoxUsuario.Text = "";
                    passwordBox.Password = "";
                    break;
                case -2:
                    MessageBox.Show("EL usuario ingresado no se encuentra activo.", "Error en el ingreso");
                    textBoxUsuario.Text = "";
                    passwordBox.Password = "";
                    break;
                case -3:
                    MessageBox.Show("Ingrese sus credenciales, no se permiten datos vacios.", "Error en el ingreso");
                    textBoxUsuario.Text = "";
                    passwordBox.Password = "";
                    break;
                //Modulo Administrador
                case 1:
                    administrador = new Modulo_Administrador(usuario);
                    this.Hide();
                    administrador.ShowDialog();
                    this.Close();
                    break;
                //Modulo operador
                case 2:
                    operador = new Modulo_Operador(usuario);
                    this.Hide();
                    operador.ShowDialog();
                    this.Close();
                    break;
                //Modulo piloto
                case 3:
                    piloto = new Modulo_Piloto(usuario);
                    this.Hide();
                    piloto.ShowDialog();
                    this.Close();
                    break;
                //Modulo consultor
                case 4:
                    consultor = new Modulo_Consultor(usuario);
                    this.Hide();
                    consultor.ShowDialog();
                    this.Close();
                    break;

            }
        }
    }
}