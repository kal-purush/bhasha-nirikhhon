package Teste;

import principal.LoginFuncionario;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
public class TesteFuncionario {

    @Test
    void testeLogin() {
        LoginFuncionario sistemaFuncionario = new LoginFuncionario(2);

        sistemaFuncionario.adicionarCredencial("atendente", "atend123", 0);
        sistemaFuncionario.adicionarCredencial("gerente", "ger123", 1);

        try {
            // Teste para autenticar com credenciais corretas
            assertTrue(sistemaFuncionario.autenticar("atendente", "atend123"));
            assertTrue(sistemaFuncionario.autenticar("gerente", "ger123"));

            // Teste para autenticar com credenciais incorretas
            assertFalse(sistemaFuncionario.autenticar("atendente", "senhaErrada"));
            assertFalse(sistemaFuncionario.autenticar("gerente", "senhaErrada"));
        } catch (Exception e) {
            fail("Exceção lançada: " + e.getMessage());
        }
    }
}