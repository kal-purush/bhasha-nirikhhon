
import java.util.Scanner;

public class Cargo {
    
    public static final int presidente = 1;
    public static final int senador = 2;
    public static final int deputadoFederal = 3;
    public static final int governador = 4;
    public static final int deputadoEstadual = 5;
    public static final int prefeito = 6;
    public static final int vereador = 7;
    
    public static String getNome(int cargo) {
        if (cargo == presidente) {
            return "Presidente";
        }
        
        if (cargo == senador) {
            return "Senador";
        }
        
        if (cargo == deputadoFederal) {
            return "Deputado Federal";
        }
        
        if (cargo == governador) {
            return "Governador";
        }
        
        if (cargo == deputadoEstadual) {
            return "Deputado Estadual";
        }
        
        if (cargo == prefeito) {
            return "Prefeito";
        }
        
        if (cargo == vereador) {
            return "Vereador";
        }
        return null;
        
    }
    
    public static int leiaMenu() {
        Scanner in = new Scanner(System.in);
        
        int opcao = 0;
        
        do {
            System.out.print("\n***SELECIONE UM CARGO***\n");
            for (int i = 1; i <= 7; i++) {
                System.out.println("[" + i + "] - " + getNome(i));
            }
            System.out.print("OPÇÃO: ");
            opcao = Integer.parseInt(in.nextLine());
            
        } while (opcao <= 0 || opcao > 7);
        
        return opcao;
    }
}