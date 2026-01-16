package br.com.fiap.soat.grupo48.application.pagamento.usecase;

import br.com.fiap.soat.grupo48.application.pagamento.model.MetodoPagamento;
import br.com.fiap.soat.grupo48.application.pagamento.port.api.IMetodoPagamentoPort;
import br.com.fiap.soat.grupo48.application.pagamento.port.spi.IMetodoPagamentoRepositoryGateway;

public class MetodoPagamentoUsecaseImpl implements IMetodoPagamentoPort {

    private final IMetodoPagamentoRepositoryGateway metodoPagamentoRepositoryGateway;

    public MetodoPagamentoUsecaseImpl(IMetodoPagamentoRepositoryGateway metodoPagamentoRepositoryGateway) {
        this.metodoPagamentoRepositoryGateway = metodoPagamentoRepositoryGateway;
    }

    @Override
    public long buscarQuantidade() {
        return this.metodoPagamentoRepositoryGateway.buscarQuantidade();
    }

    @Override
    public MetodoPagamento salvar(MetodoPagamento metodoPagamento) {
        return this.metodoPagamentoRepositoryGateway.salvar(metodoPagamento.getCodigo(),metodoPagamento.getNome(),metodoPagamento.getTipoPagamento(),metodoPagamento.getUrlImagem());
    }
}