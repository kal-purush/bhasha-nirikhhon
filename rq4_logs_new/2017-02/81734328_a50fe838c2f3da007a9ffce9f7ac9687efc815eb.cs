using AutoMapper;
using GerenciadorMongo.Apresentacao.Models;
using GerenciadorMongo.Dominio.Entidades;
using System.Windows;

namespace GerenciadorMongo.Apresentacao
{
    /// <summary>
    /// Interação lógica para App.xaml
    /// </summary>
    public partial class App : Application
    {
        protected override void OnStartup(StartupEventArgs e)
        {
            Mapper.Initialize(cfg => 
            {
                cfg.CreateMap<Cliente, ClienteModel>().MaxDepth(1);
                cfg.CreateMap<ClienteModel, Cliente>().MaxDepth(1);

                cfg.CreateMap<Produto, ProdutoModel>().MaxDepth(1);
                cfg.CreateMap<ProdutoModel, Produto>().MaxDepth(1);

                cfg.CreateMap<Pedido, PedidoModel>().MaxDepth(1);
                cfg.CreateMap<PedidoModel, Pedido>().MaxDepth(1);

                cfg.CreateMap<PedidoProduto, PedidoProdutoModel>().MaxDepth(1);
                cfg.CreateMap<PedidoProdutoModel, PedidoProduto>().MaxDepth(1);

                cfg.CreateMap<ClienteModel, ClienteModel>().MaxDepth(1);
                cfg.CreateMap<ProdutoModel, ProdutoModel>().MaxDepth(1);
                cfg.CreateMap<PedidoModel, PedidoModel>().MaxDepth(1);
                cfg.CreateMap<PedidoProdutoModel, PedidoProdutoModel>().MaxDepth(1);
            });

            base.OnStartup(e);
        }
    }
}