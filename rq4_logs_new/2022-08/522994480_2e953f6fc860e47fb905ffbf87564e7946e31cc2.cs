using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using WpfApp3.core;

namespace WpfApp3.MVVM.ViewModel
{
    class DeletarProduto : BaseCommand
    {
        public override bool CanExecute(object parameter)
        {
            var viewModel = parameter as CadastroProdutoViewModel;
            return viewModel != null && viewModel.ProdutoSelecionado != null;
        }

        public override void Execute(object parameter)
        {
            var viewModel = (CadastroProdutoViewModel)parameter;
            viewModel.Produtos.Remove(viewModel.ProdutoSelecionado);
            viewModel.ProdutoSelecionado = viewModel.Produtos.FirstOrDefault();

            string jsonString = JsonSerializer.Serialize(viewModel.Produtos, new JsonSerializerOptions() { WriteIndented = true });
            using (StreamWriter outputFile = new StreamWriter("produto.json"))
            {
                outputFile.WriteLine(jsonString);
            }
        }
    }
}