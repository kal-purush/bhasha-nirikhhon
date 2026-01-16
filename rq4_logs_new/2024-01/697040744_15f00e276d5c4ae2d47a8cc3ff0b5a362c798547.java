package br.com.fiap.soat.grupo48.application.pagamento.port.spi;

import br.com.fiap.soat.grupo48.application.pagamento.model.MetodoPagamento;
import br.com.fiap.soat.grupo48.application.pagamento.model.Pagamento;
import br.com.fiap.soat.grupo48.application.pagamento.model.TipoPagamento;

public interface IConsultaInformacaoPagamentoIntegartionGateway {

    Pagamento buscarSituacaoPagamento(TipoPagamento tipoPagamento, String id);
}