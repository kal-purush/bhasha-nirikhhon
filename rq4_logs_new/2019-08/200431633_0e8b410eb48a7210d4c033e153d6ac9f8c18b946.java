
package teste;

import java.util.Scanner;

public class Lanche {


     
    public static void main(String[] args) {
         int pedido;
         
       System.out.println("======= LANCHONETE =======" + "\n");
       System.out.println("===== LANCHES DA BAHIA  =====");
       System.out.println("Nº 1 -  Acarajé R$: 3,50");
       System.out.println("Nº 2 -  Abará R$: 2,50");
       System.out.println("Nº 3 -  Cocada R$: 1,50");
       System.out.println("Nº 4 -  Bolinho de Estudante R$: 1,50");
       System.out.println("Nº 5 -  Passarinha R$: 2,50" + "\n");
        
       Scanner captura = new Scanner(System.in);
         System.out.print("Digite o número do Lanche: ");
         pedido = captura.nextInt();
         
   
         
         switch (pedido) {
        case  1 :
          System.out.println("Você escolheu Acarajé!");
          System.out.println("Parabéns!");
          break;
          case  2:
                System.out.println("Você escolheu Abará!");
                 System.out.println("Parabéns!");
                break;
          case 3:
              System.out.println("Você escolheu Cocada!");
               System.out.println("Parabéns!");
              break;
          case 4:
               System.out.println("Você escolheu Bolinho de Estudante!");
                System.out.println("Parabéns!");
               break;
          case 5:
              System.out.println("Você escolheu Passarinha!");
               System.out.println("Parabéns!");
              break;
          default:
              System.out.println("Número Errado!");
               System.out.println("Digite um Número Válido!");
              break;
             
              
         }
               
    }
         
}