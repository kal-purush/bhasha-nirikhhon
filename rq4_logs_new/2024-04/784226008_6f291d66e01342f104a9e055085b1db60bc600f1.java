package Teste;

import entidades.Cliente;
import entidades.Fornecedor;
import entidades.Funcionario;
import exibirDados.ExibirDados;
import org.junit.jupiter.api.Test;
import utilitarios.LimparTela;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import static org.junit.Assert.assertNotNull;

public class TesteCadastro {

    @Test
    void TesteCadastroCliente() {

        List<Cliente> clientes = new ArrayList<Cliente>();
        System.out.println("\n--------- Cadastro de Cliente ---------\n");

        System.out.print("CPF do cliente (000.000.000-00): ");
        String cpf = "123.456.789-00";

        System.out.print("Nome completo do cliente: ");
        String nome = "João Silva";

        System.out.print("Data de nascimento do cliente (00/00/0000): ");
        String dataNascimento = "17/01/2004";

        System.out.print("Endereço do cliente (rua, bairro, cidade): ");
        String endereco = "Rua João Guimarães,Peixinho,Xique-Xique";

        System.out.print("Telefone do cliente (00 00000-0000): ");
        String telefone = "93 2395-7237";

        System.out.print("E-mail do cliente: ");
        String email = "Joaozinho17@gmail.com";

        Cliente cliente = new Cliente(nome, cpf, dataNascimento, endereco, telefone, email);
        clientes.add(cliente);

        System.out.println("\nSituação: Cliente cadastrado com sucesso!\n");
        ExibirDados.exibirDadosCliente(cliente);
        assertNotNull(clientes);
    }
    @Test
    void TesteCadastroFuncionario() {
        List<Funcionario> funcionarios = new ArrayList<Funcionario>();

        System.out.println("\n--------- Cadastro de Funcionário ---------\n");
        System.out.print("Nome completo do funcionário: ");
        String nome = "Carlos Cesar";

        System.out.print("CPF do funcionário (000.000.000-00): ");
        String cpf = "987.654.321-01";

        System.out.print("Endereço do funcionário (rua, bairro, cidade): ");
        String endereco ="Rua Cornélio Barros, Jardim Planalto, Xique-Xique";

        System.out.print("Data de nascimento do funcionário (00/00/0000): ");
        String dataNascimento ="25/03/1999";

        System.out.print("Cargo do funcionário: ");
        String cargo = "Cozinheiro";

        System.out.print("Data de admissão do funcionário (00/00/0000): ");
        String dataAdmissao = "01/05/2015";

        System.out.print("Telefone do funcionário (00 00000-0000): ");
        String telefone = "94 985403-5525";

        System.out.print("E-mail do funcionário: ");
        String email = "carlinhosgera@gmail.com";

        System.out.print("Salário do funcionário: ");
        double salario = 1277.58;

        System.out.print("Tipo de contrato do funcionário (integral/indeterminado/meio período/temporário/estágio/jovem aprendiz): ");
        String tipoContrato = "Integral";

        System.out.print("Turno do funcionário (manhã/tarde/noite): ");
        String turno = "Manhã";

        Funcionario funcionario = new Funcionario(nome, dataNascimento, salario, cpf, endereco, cargo, dataAdmissao, telefone, email, turno, tipoContrato);
        funcionarios.add(funcionario);
        assertNotNull(funcionarios);

        System.out.println("\nSituação: Funcionário cadastrado com sucesso!\n");
        ExibirDados.exibirDadosFuncionario(funcionario);
    }

    @Test
    void TesteCadastroFornecedor() {

        List<Fornecedor> fornecedores = new ArrayList<Fornecedor>();

        System.out.println("\n--------- Cadastro de Fornecedor ---------\n");
        System.out.print("Nome completo do fornecedor: ");
        String nome = "SalgadoXX";

        System.out.print("CNPJ do fornecedor (00.000.000/0001-00): ");
        String cnpj =  "74.551.464/0001-22";

        System.out.print("Endereço do fornecedor (rua, bairro, cidade): ");
        String endereco = "Rua Aqueduto Reinaldinho,Lar dos Mariscos,Xique-Xique";

        System.out.print("Telefone do fornecedor (00 00000-0000): ");
        String telefone = "93 2341-1824";

        System.out.print("E-mail do fornecedor: ");
        String email =  "salgadoscia@gmail.com";

        System.out.print("Produto(s) fornecido pelo fornecedor (separe por vírgula): ");
        String produtoFornecido = "Coxinha";

        Fornecedor fornecedor = new Fornecedor(nome, cnpj, endereco, telefone, email, produtoFornecido);
        fornecedores.add(fornecedor);

        System.out.println("\nSituação: Fornecedor cadastrado com sucesso!\n");
        ExibirDados.exibirDadosFornecedor(fornecedor);

        assertNotNull(fornecedores);
        System.out.println("\n------------------------------------------");
    }
}