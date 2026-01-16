package br.com.fiap.soat.grupo48.infrastructure.adapter.driver.rest.pedido;

import br.com.fiap.soat.grupo48.application.cliente.model.Cliente;
import br.com.fiap.soat.grupo48.application.pedido.model.Pedido;
import br.com.fiap.soat.grupo48.application.pedido.model.PedidoItem;
import br.com.fiap.soat.grupo48.application.pedido.model.SituacaoPedido;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class PedidoEmAndamentoDTOMapper {

    public Pedido toPedido(PedidoRequest request) {
        Cliente cliente = null;
        SituacaoPedido situacao =null;
        List<PedidoItem> itens = null;
        return new Pedido(request.getCodigo(),cliente, situacao,request.getNumero(),itens);
    }
}