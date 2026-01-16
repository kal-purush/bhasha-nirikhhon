using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Windows;
using System.Windows.Controls;
using WpfApp3.MVVM.Model;

namespace WpfApp3.MVVM.View
{
    /// <summary>
    /// Interaction logic for CadastroPessoa1.xaml
    /// </summary>
    public partial class CadastroPessoa1 : UserControl
    {
        public CadastroPessoa1()
        {
            InitializeComponent();
        }
       
       
        public void salvar(object sender, RoutedEventArgs e)
        {
            /*Pessoa pessoa = new Pessoa();
            pessoa.Nome = Nome.Text;
            pessoa.Id = 1;
            pessoa.Cpf = Cpf.Text;
            pessoa.Endereco = Endereco.Text;*/


            List<Pessoa> source = new List<Pessoa>();

            using (StreamReader r = new StreamReader("pessoa.json"))
            {
                string json = r.ReadToEnd();
                source = JsonSerializer.Deserialize<List<Pessoa>>(json);
            }

            /*source.Add(pessoa);*/

            string jsonString = JsonSerializer.Serialize(source, new JsonSerializerOptions() { WriteIndented = true });
            using (StreamWriter outputFile = new StreamWriter("pessoa.json"))
            {
                outputFile.WriteLine(jsonString);
            }
        }
    }
}