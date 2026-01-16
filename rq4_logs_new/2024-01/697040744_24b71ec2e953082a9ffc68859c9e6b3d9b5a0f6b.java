package br.com.fiap.soat.grupo48.infrastructure.adapter.driven.persistence.repository.pedido;

import br.com.fiap.soat.grupo48.application.pedido.model.SituacaoPedido;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

@Converter(autoApply = true)
public class SituacaoPedidoConverter implements AttributeConverter<SituacaoPedido, String> {

    private static final Map<String, SituacaoPedido> situacoes = Stream.of(SituacaoPedido.values())
            .collect(toMap(SituacaoPedido::name, Function.identity()));

    @Override
    public String convertToDatabaseColumn(SituacaoPedido situacaoPedido) {
        return situacaoPedido.name();
    }

    @Override
    public SituacaoPedido convertToEntityAttribute(String s) {
        return situacoes.getOrDefault(s, SituacaoPedido.EM_PREPARACAO);
    }
}