package pedidos;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Scanner;

import produto.ProdutoDisponivel;
import utilitarios.LimparTela;

public class EfetuarPedido {
	
	private static ArrayList<Pedido> pedidosEmAndamento = new ArrayList<>();
    private static ArrayList<Pedido> historicoDePedidos = new ArrayList<>();
    private static ArrayList<ProdutoDisponivel> produtosDisponiveis = new ArrayList<>();
	private static int proximoNumeroPedido = 1;


    public static void efetuarPedido() {
    	
    	LimparTela.DeixarVazio();

    	Scanner sc = new Scanner(System.in);
    	
    	produtosDisponiveis.add(new ProdutoDisponivel("Hambúrguer com Cheddar", 10.00));
    	produtosDisponiveis.add(new ProdutoDisponivel("Cachorro Quente", 4.00));
    	produtosDisponiveis.add(new ProdutoDisponivel("Batata Frita pote médio", 6.00));
    	produtosDisponiveis.add(new ProdutoDisponivel("Refrigerante 1L", 3.00));
    	produtosDisponiveis.add(new ProdutoDisponivel("Mini Pizza", 3.00));
    	produtosDisponiveis.add(new ProdutoDisponivel("Esfiha", 2.50));
    	produtosDisponiveis.add(new ProdutoDisponivel("Coxinha", 3.50));
    	produtosDisponiveis.add(new ProdutoDisponivel("Empada Doce", 2.50));
    	produtosDisponiveis.add(new ProdutoDisponivel("Empada Salgada", 2.50));
    	produtosDisponiveis.add(new ProdutoDisponivel("Enroladinho", 4.00));
    	produtosDisponiveis.add(new ProdutoDisponivel("Torta Salgada", 5.00));
    	produtosDisponiveis.add(new ProdutoDisponivel("Água", 1.50));

    	ArrayList<Integer> quantidades = new ArrayList<>();
    	ArrayList<String> produtosSelecionados = new ArrayList<>();

    	System.out.println("\n-------- Cardápio da lanchonete -------\n");

    	for (int i = 0; i < produtosDisponiveis.size(); i++) {
    		ProdutoDisponivel produto = produtosDisponiveis.get(i);
    		System.out.println((i + 1) + ". " + produto.getNome() + " - R$ " + produto.getPreco());
    	}

    	int produtoIndex;
    	boolean finalizarSelecao = false;

    	while (true) {
    		System.out.print("\nDigite o número correspondente ao(s) produto(s) desejado(s) pelo cliente (ou '0' para finalizar): ");
    		String inputProduto = sc.next().trim();

    		if (inputProduto.equals("0")) {
    			finalizarSelecao = true;
    			if (produtosSelecionados.isEmpty()) {
    				System.out.println("\nNenhum produto selecionado. Pedido cancelado.");
    				System.out.print("\n-----------------------------\n");
    				return;
    			}
    			break;
    		}

    		String[] opcoes = inputProduto.split(",");
    		boolean ocorreuErro = false;

    		for (String opcao : opcoes) {
    			try {
    				produtoIndex = Integer.parseInt(opcao.trim()) - 1;

    				if (produtoIndex >= 0 && produtoIndex < produtosDisponiveis.size()) {
    					System.out.print("\nDigite a quantidade de " + produtosDisponiveis.get(produtoIndex).getNome() + " desejada pelo cliente: ");
    					int quantidade;
    					try {
    						quantidade = Integer.parseInt(sc.next());
    					} catch (NumberFormatException e) {
    						System.out.println("\nA quantidade digitada é inválida. Digite um número válido.");
    						return;
    					}

    					if (quantidade <= 0) {
    						System.out.println("\nQuantidade inválida. Digite um número maior que zero.");
    						return;
    					}

    					quantidades.add(quantidade);
    					produtosSelecionados.add(String.valueOf(produtoIndex + 1));

    				} else {
    					System.out.println("\nA opção " + opcao + " digitada é inválida.");
    					ocorreuErro = true;
    					break;
    				}
    			} catch (NumberFormatException e) {
    				System.out.println("\nA opção " + opcao + " digitada é inválida. Opção ignorada.");
    				ocorreuErro = true;
    			}
    		}

    		if (ocorreuErro) {
    			System.out.println("\nDigite novamente!");
    		}
    	}

    	if (finalizarSelecao) {
    		System.out.println("\nTotal de itens no pedido: " + quantidades.size());
    		System.out.print("\n-------------------------------\n");
    	}

    	System.out.print("\nDigite o nome do cliente: ");
    	sc.nextLine();
    	String nomeCliente = sc.nextLine();

    	double valorTotal = 0;

    	for (int i = 0; i < produtosSelecionados.size(); i++) {
    		produtoIndex = Integer.parseInt(produtosSelecionados.get(i)) - 1;
    		double precoProduto = produtosDisponiveis.get(produtoIndex).getPreco();
    		int quantidadeProduto = quantidades.get(i);
    		valorTotal += precoProduto * quantidadeProduto;
    	}
    	
    	int numeroPedido = proximoNumeroPedido;
    	proximoNumeroPedido++;

    	String descricaoPedido = "";

    	for (int i = 0; i < produtosSelecionados.size(); i++) {
    		produtoIndex = Integer.parseInt(produtosSelecionados.get(i)) - 1;
    		String nomeProduto = produtosDisponiveis.get(produtoIndex).getNome();
    		int quantidadeProduto = quantidades.get(i);
    		descricaoPedido += quantidadeProduto + "x " + nomeProduto + ", ";
    	}

    	descricaoPedido = descricaoPedido.substring(0, descricaoPedido.length() - 2);

    	Pedido novoPedido = new Pedido(numeroPedido, nomeCliente, quantidades, produtosSelecionados.toArray(new String[0]), produtosDisponiveis);
    	novoPedido.setDescricao(descricaoPedido);
    	pedidosEmAndamento.add(novoPedido);
    	historicoDePedidos.add(novoPedido);

    	System.out.printf("\nTotal do Pedido: R$ %.2f\n", valorTotal);
    	System.out.println("\nPedido de número " + numeroPedido + " criado!");
    	System.out.print("\n-----------------------------\n");
    }
	public static void cancelarPedido() {
		
		LimparTela.DeixarVazio();
		Scanner sc = new Scanner(System.in);
		
		if (pedidosEmAndamento.isEmpty()) {
            System.out.println("---------- Cancelamento de pedidos ----------\n");
            System.out.println("Não existem pedidos em andamento para cancelar.");
            System.out.print("\n-----------------------------\n");
        } else {
            System.out.println("---------- Cancelamento de pedidos ----------\n");
            System.out.print("\nDigite o número do pedido para cancelar: ");
            int numeroPedidoParaCancelar = sc.nextInt();
            boolean pedidoEncontrado = false;

            for (Pedido pedido : pedidosEmAndamento) {
                if (pedido.getNumeroPedido() == numeroPedidoParaCancelar) {
                    System.out.println("\nPedido cancelado!");
                    pedido.setStatus("Pedido cancelado");
                    pedidoEncontrado = true;
                    break;
                }
            }

            if (pedidoEncontrado) {
                pedidosEmAndamento.removeIf(p -> p.getStatus().equals("Pedido cancelado"));
            } else {
                System.out.println("\nNúmero de pedido inválido.");
            }
            System.out.print("\n-----------------------------\n");
        }
	}
	public static void finalizarPedidos() {
    	
    	LimparTela.DeixarVazio();
    	Scanner sc = new Scanner(System.in);
    	
        if (pedidosEmAndamento.isEmpty()) {
            System.out.println("\n------- Pedidos em Andamento para Finalizar --------\n\n\nNão existem pedidos em andamento para finalizar.\n\n-----------------------------");

        } else {
            System.out.println("\n------- Pedidos em Andamento para Finalizar --------\n");
            Iterator<Pedido> iterator = pedidosEmAndamento.iterator();

            while (iterator.hasNext()) {
                Pedido pedido = iterator.next();
                System.out.println("Número do Pedido: " + pedido.getNumeroPedido());
                System.out.println("Descrição do Pedido: " + pedido.getDescricao());
                System.out.println("Nome do Cliente: " + pedido.getNomeCliente());
                System.out.println("Status: " + pedido.getStatus());

                System.out.print("\nDigite 'ok' para finalizar este pedido: ");
                String resposta = sc.next().toLowerCase();
                if (resposta.equals("ok")) {
                    System.out.println("\nPedido finalizado!\n");
                    
                    for (Pedido historicoPedido : historicoDePedidos) {
                        if (historicoPedido.getNumeroPedido() == pedido.getNumeroPedido()) {
                            historicoPedido.setStatus("Pedido finalizado");
                            break;
                        }
                    }

                    iterator.remove();
                }
                System.out.print("\n-----------------------------\n");
            }
        }
    }
	public static void exibirHistorico() {
		
		LimparTela.DeixarVazio();
		
        if (historicoDePedidos.isEmpty()) {
            System.out.println("\n------- Histórico de Pedidos --------\n");
            System.out.println("Nenhum pedido no histórico.");
            System.out.print("\n-----------------------------\n");
            
        } else {
            System.out.println("\n------- Histórico de Pedidos --------\n");

            for (Pedido pedido : historicoDePedidos) {
                System.out.println("Número de Pedido: " + pedido.getNumeroPedido());
                System.out.println("Nome do Cliente: " + pedido.getNomeCliente());
                System.out.println("Descrição do Pedido: " + pedido.getDescricao()); 
                System.out.println("Status: " + pedido.getStatus());

                System.out.print("\n-----------------------------\n");
            }
        }
    }

}