package Teste;

import static org.junit.jupiter.api.Assertions.*;
import entidades.Cliente;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class TesteListagens {

    @Test
    void ListarClientes(){
        List<Cliente> clientes = new ArrayList<Cliente>();

        Cliente cliente = new Cliente("João Silva","123.456.789-00","17/01/2004",
                "Rua João Guimarães,Peixinho,Xique-Xique","93 2395-7237","Joaozinho17@gmail.com");

        clientes.add(cliente);
        assertNotNull(clientes);
    }
}
