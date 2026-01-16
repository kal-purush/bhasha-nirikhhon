/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package salariobase;

import java.util.Scanner;

/**
 *
 * @author SIMONE ABREU
 */
public class SalarioBase {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
      int salarioBase, gratificacao, imposto, salarioLiquido;
          Scanner entrada = new Scanner(System.in);  
      
          System.out.println("Informe o valor do salario base:");
          salarioBase = entrada.nextInt();
          entrada.close();
          gratificacao = (int) (salarioBase * 0.05);
          imposto = (int) (salarioBase * 0.03);
          salarioLiquido = salarioBase + gratificacao - imposto;
          System.out.println("O salario liquido é " + salarioLiquido);
    }

    
    
}
 