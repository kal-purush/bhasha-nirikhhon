import java.util.Scanner;

public class ContaTerminal {
    public static void main(String[] args) throws Exception {
        Scanner sc = new Scanner(System.in);

        
        System.out.println("Por favor, digite o número da Agência !");
        String agencia = sc.next();
        System.out.println("Por favor, digite o número da Conta !");
        int numeroConta = sc.nextInt();
        sc.nextLine();
        System.out.println("Por favor, informe seu Nome !");
        String nomeCliente = sc.nextLine();
        System.out.println("Por favor, informe seu saldo atual !");
        double saldo = sc.nextDouble();


        System.out.printf("Olá %s, obrigado por criar uma conta em nosso banco, sua agência é %s, conta %d e seu saldo %.2f já está disponível para saque", nomeCliente, agencia, numeroConta, saldo);
        sc.close();
    }
}