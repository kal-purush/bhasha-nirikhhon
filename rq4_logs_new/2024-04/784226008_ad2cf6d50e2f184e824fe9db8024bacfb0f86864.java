package cadastros;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import entidades.Cliente;
import exibirDados.ExibirDados;
import interfaces.CadRemLis;
import utilitarios.LimparTela;

public class CadastroDeClientes implements CadRemLis{

	private List<Cliente> clientes = new ArrayList<Cliente>();
	private static int proximoNumeroPedido = 1;
	
	// Cadastrar cliente;
	@Override
	public void Cadastrar() {
		
		//public 
		
		Scanner sc = new Scanner(System.in);
		LimparTela.DeixarVazio();
		
	    System.out.println("\n--------- Cadastro de Cliente ---------\n");
	
		System.out.print("CPF do cliente (000.000.000-00): ");
		String cpf = sc.nextLine();
		
		System.out.print("Nome completo do cliente: ");
		String nome = sc.nextLine();
	
		System.out.print("Data de nascimento do cliente (00/00/0000): ");
		String dataNascimento = sc.nextLine();
		
		System.out.print("Endereço do cliente (rua, bairro, cidade): ");
		String endereco = sc.nextLine();
	
		System.out.print("Telefone do cliente (00 00000-0000): ");
		String telefone = sc.nextLine();
	
		System.out.print("E-mail do cliente: ");
		String email = sc.nextLine();
	
		Cliente cliente = new Cliente(nome, cpf, dataNascimento, endereco, telefone, email);
		clientes.add(cliente);
	
		System.out.println("\nSituação: Cliente cadastrado com sucesso!\n");
		ExibirDados.exibirDadosCliente(cliente);
		
		//sc.close();
	}

	@Override
	public void Remover() {

		Scanner sc = new Scanner(System.in);
		LimparTela.DeixarVazio();
	    
		System.out.println("--------- Remover Cliente ---------\n");
	    
	    // Verifica se a lista de clientes está vazia
	    if (clientes.isEmpty()){
	    	System.out.println("Não há clientes cadastrados para remover.\n");
	        System.out.println("-------------------------------------\n");
	    }
	    else {
	    	System.out.print("Digite o CPF do cliente a ser removido (000.000.000-00): ");
		    String cpf = sc.nextLine();

		    Cliente clienteParaRemover = null; //Null enquanto o cliente não for encontrado
		    
		    for (Cliente cliente : clientes) { // For each para percorrer todos os clientes da lista 
		        if (cliente.getCpf().equals(cpf)) { // Verifica se o CPF digitado correspondente ao CPF do cliente que deseja ser excluído 
		            clienteParaRemover = cliente; // Cliente desejado excluído da lista 
		            break;
		        }
		    }
		    if (clienteParaRemover != null) { //Null enquanto o cliente não for encontrado
		        String nomeCliente = clienteParaRemover.getNome(); //Obtém o nome do cliente que deseja ser excluído 
		        clientes.remove(clienteParaRemover); // Cliente excluído da lista 
		        System.out.println("\nSituação: Cliente " + nomeCliente + " com CPF " + cpf + " foi excluído com sucesso!\n");
		        System.out.println("-------------------------------------\n");
		    } else {
		        System.out.println("\nSituação: Cliente não encontrado!\n");
		        System.out.println("-------------------------------------\n");
		    }
	    }		
	}

	@Override
	public void Listar() {
		
		CadastroDeClientes cadastro = new CadastroDeClientes();
		LimparTela.DeixarVazio();
		
	    System.out.println("--------- Clientes Cadastrados --------- \n");
	
	    // Verifica se a lista de clientes está vazia
	    if (clientes.isEmpty()) {
	        System.out.println("Não há clientes cadastrados para listar.\n");
	        System.out.println("-------------------------------------\n");
	        return; // Sai do método, pois não há clientes para listar
	    }
	
	    int numeroCliente = 1; // Número do cliente começa em 1 
	
	    for (Cliente cliente : clientes) {
	        System.out.println("Cliente " + numeroCliente + "\n"); // Enumera os clientes 
	        ExibirDados.exibirDadosCliente(cliente);// Exibe as informações dos clientes 
	        System.out.println("\n------------------------------------------\n");
	        numeroCliente++; // Incremeta em 1 o número do cliente 
	    }		
	}

}