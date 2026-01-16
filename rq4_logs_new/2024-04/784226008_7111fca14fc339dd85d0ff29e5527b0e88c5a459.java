package Teste;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import pedidos.EfetuarPedido;
import pedidos.Pedido;


import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

public class TesteRemocoes{

    @Test
    void FinalizarPedidoVazio() {

        String saidaEsperada = "\n------- Pedidos em Andamento para Finalizar --------\n\n\nNão existem pedidos em andamento para finalizar.\n\n-----------------------------\n";

        String saidaAtual = capturarSaidaDoConsole();
        assertEquals(saidaEsperada, saidaAtual);
    }

    private String capturarSaidaDoConsole() {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        PrintStream saidaOriginal = System.out;

        try {
            System.setOut(new PrintStream(buffer, true, "UTF-8")); // Redireciona a saída para o buffer

            // Executa o código que gera a saída do console (neste caso, o método a ser testado)
            EfetuarPedido.finalizarPedidos();
            // Obtém a saída capturada do buffer
            String saidaCapturada = buffer.toString("UTF-8");

            // Restaura a saída original do console
            System.setOut(saidaOriginal);

            return saidaCapturada;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        } finally {
            // Limpa o buffer para evitar acúmulo de dados
            buffer.reset();
        }
    }

}