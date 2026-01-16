package com.api.cep;

import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

import com.api.cep.entity.EnderecoViaCep;
import com.api.cep.service.ApiViaCepService;
import com.api.cep.service.EnderecoViaCepService;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class EnderecoViaCepServiceTest {

    @Mock
    private ApiViaCepService apiViaCepService;

    @InjectMocks
    private EnderecoViaCepService enderecoViaCepService;

    @Test
    void testFindByCep_ValidCep_ShouldReturnEndereco() {
        // Arrange
        String validCep = "12345-678";
        EnderecoViaCep endereco = new EnderecoViaCep("Rua ABC", "123", "Bairro XYZ", "Cidade", "Estado");
        when(apiViaCepService.buscaCepApi(validCep + "/json/")).thenReturn(Mono.just(endereco));

        // Act
        Mono<EnderecoViaCep> result = enderecoViaCepService.findByCep(validCep);

        // Assert
        assertNotNull(result);
        assertTrue(result.block() instanceof EnderecoViaCep);
        assertEquals("Rua ABC", result.block().getLogradouro());
        verify(apiViaCepService, times(1)).buscaCepApi(validCep + "/json/");
    }

    @Test
    void testFindByCep_InvalidCep_ShouldReturnError() {
        // Arrange
        String invalidCep = "12345678"; // Formato inválido
        // Não há necessidade de mockar a resposta, pois esperamos um erro

        // Act
        Mono<EnderecoViaCep> result = enderecoViaCepService.findByCep(invalidCep);

        // Assert
        assertThrows(IllegalArgumentException.class, result::block); // Espera uma IllegalArgumentException ser lançada
    }

    @Test
    void testIsValidCEP_ShouldReturnTrueForValidCep() {
        // Arrange
        String validCep = "12345-678";

        // Act
        boolean isValid = EnderecoViaCepService.isValidCEP(validCep);

        // Assert
        assertTrue(isValid);
    }

    @Test
    void testIsValidCEP_ShouldReturnFalseForInvalidCep() {
        // Arrange
        String invalidCep = "12345678";

        // Act
        boolean isValid = EnderecoViaCepService.isValidCEP(invalidCep);

        // Assert
        assertFalse(isValid);
    }
}