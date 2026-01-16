package br.com.mercado.domain.mapper;

import br.com.mercado.domain.dto.request.FornecedorRequest;
import br.com.mercado.domain.dto.response.FornecedorResponse;
import br.com.mercado.domain.model.Fornecedor;

public class FornecedorMapper {
    public static FornecedorResponse toResponse(Fornecedor fornecedor) {
        return FornecedorResponse.builder()
                .nome(fornecedor.getNome())
                .cnpj(fornecedor.getCnpj())
                .build();
    }
    public static Fornecedor toEntity(FornecedorRequest request) {
        return Fornecedor.builder()
                .nome(request.getNome())
                .cnpj(request.getCnpj())
                .build();
    }


}