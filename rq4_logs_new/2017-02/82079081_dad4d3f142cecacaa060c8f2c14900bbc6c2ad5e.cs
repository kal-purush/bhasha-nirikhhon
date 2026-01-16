using Contoso.Shop.EFTests.Shop;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace Contoso.Shop.EFTests.ViewModels
{
    public class ListaProdutosViewModel
    {
        public ListaProdutosViewModel() {
            Produtos = Array.Empty<Produto>();
        }

        [Display(Name = "Termo Pesquisa")]
        public string TermoPesquisa { get; set; }
        public IEnumerable<Produto> Produtos { get; set; }
    }
}