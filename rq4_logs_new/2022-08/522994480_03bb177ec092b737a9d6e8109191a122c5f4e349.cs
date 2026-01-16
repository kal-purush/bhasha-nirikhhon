using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using WpfApp3.core;
using WpfApp3.MVVM.Model;
using WpfApp3.MVVM.ViewModel;

namespace WpfApp3.MVVM.crud
{
    class EditarProduto : BaseCommand
    {
        public override bool CanExecute(object parameter)
        {
            var viewModel = parameter as ProdutoViewModel;
            return viewModel != null && viewModel.ProdutoSelecionado != null;
        }

        public override void Execute(object parameter)
        {
            var viewModel = (ProdutoViewModel)parameter;
           
            var cloneProduto = (Model.Produto)viewModel.ProdutoSelecionado.Clone();
             
            viewModel.ProdutoEdit.Id = cloneProduto.Id;
            viewModel.ProdutoEdit.Nome = cloneProduto.Nome;
            viewModel.ProdutoEdit.Codigo = cloneProduto.Codigo;
            viewModel.ProdutoEdit.Valor = cloneProduto.Valor;

            int contador = 0;
            int posicao = -1;
            foreach (Produto produto in viewModel.Produtos)
            {
                if (produto.Id == viewModel.ProdutoEdit.Id)
                {
                    posicao = contador;
                }
                contador++;
            }

            viewModel.Produtos[posicao] = viewModel.ProdutoEdit;

            string jsonString = JsonSerializer.Serialize(viewModel.Produtos, new JsonSerializerOptions() { WriteIndented = true });
            using (StreamWriter outputFile = new StreamWriter("produto.json"))
            {
                outputFile.WriteLine(jsonString);
            }
        }
    }
}