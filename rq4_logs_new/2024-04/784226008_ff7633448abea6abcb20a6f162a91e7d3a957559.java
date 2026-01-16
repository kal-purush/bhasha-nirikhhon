package Teste;

import entidades.Funcionario;
import entidades.Fornecedor;
import entidades.Cliente;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

public class TesteExibirDados {

    @Test
    void testeExibirDdaosCliente(){

        Cliente cliente = new Cliente("João Silva","123.456.789-00","17/01/2004",
                "Rua João Guimarães,Peixinho,Xique-Xique","93 2395-7237","Joaozinho17@gmail.com");

        assertEquals("João Silva",cliente.getNome());
        assertEquals("123.456.789-00",cliente.getCpf());
        assertEquals("17/01/2004",cliente.getDataNascimento());
        assertEquals("Rua João Guimarães,Peixinho,Xique-Xique",cliente.getEndereco());
        assertEquals("93 2395-7237",cliente.getTelefone());
        assertEquals("Joaozinho17@gmail.com",cliente.getEmail());
    }

    @Test
    void testeExibirDdaosFornecedores(){

        Fornecedor fornecedor = new Fornecedor("74.551.464/0001-22","SalgadoXX","Rua Aqueduto Reinaldinho,Lar dos Mariscos,Xique-Xique",
                "93 2341-1824","salgadoscia@gmail.com","Coxinha");

        assertEquals("74.551.464/0001-22",fornecedor.getCnpj());
        assertEquals("SalgadoXX",fornecedor.getNomeEmpresa());
        assertEquals("Rua Aqueduto Reinaldinho,Lar dos Mariscos,Xique-Xique",fornecedor.getEndereco());
        assertEquals("93 2341-1824",fornecedor.getTelefone());
        assertEquals("salgadoscia@gmail.com",fornecedor.getEmail());
        assertEquals("coxinha",fornecedor.getEmail());
    }

    @Test
    void  testeExibirDdaosFuncionarios(){

        Funcionario funcionario = new Funcionario("987.654.321-01","Carlos Cesar",1277.58,
                "25/03/1999","94 985403-5525","Rua Cornélio Barros, Jardim Planalto, Xique-Xique","Cozinheiro","01/05/2015","carlinhosgera@gmail.com","Integral","CLT");

        assertEquals("987.654.321-01",funcionario.getCpf());
        assertEquals("Carlos Cesar",funcionario.getNome());
        assertEquals(1277.58,funcionario.getSalario());
        assertEquals("25/03/1999",funcionario.getDataNascimento());
        assertEquals("94 985403-5525",funcionario.getTelefone());
        assertEquals("Rua Cornélio Barros, Jardim Planalto, Xique-Xique",funcionario.getEndereco());
        assertEquals("Cozinheiro",funcionario.getCargo());
        assertEquals("01/05/2015",funcionario.getDataAdmissao());
        assertEquals("carlinhosgera@gmail.com",funcionario.getEmail());
        assertEquals("Integral",funcionario.getTurno());
        assertEquals("CLT",funcionario.getTipoContrato());



    }
}
