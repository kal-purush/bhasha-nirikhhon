package modelo;


public class Venda {

    private CarrinhoCompras carrinho;
    private Pedido pedido;

    public CarrinhoCompras getCarrinho() {
        return carrinho;
    }

    public void setCarrinho(CarrinhoCompras carrinho) {
        this.carrinho = carrinho;
    }

    public Pedido getPedido() {
        return pedido;
    }

    public void setPedido(Pedido pedido) {
        this.pedido = pedido;
    }

    public CarrinhoCompras addItem(int quantidade, Produto produto) {

        CarrinhoCompras carrinho = new CarrinhoCompras();
        carrinho.adicionarItem(quantidade, produto);

        return carrinho;
    }

    public Pedido Finalizar(CarrinhoCompras carrinho) {
        Pedido pedido = new Pedido();
        pedido.setDataCompra("20/11/2016");
        pedido.setItens(carrinho.getItens());

        return pedido;
    }
}