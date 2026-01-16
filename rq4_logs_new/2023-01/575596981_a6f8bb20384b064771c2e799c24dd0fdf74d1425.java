package EV1.P_5.P_5_2;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Practica5_2_1 {
    public static void main(String[] args) {
        int cont = 1;
        int[] v1 = new int[6];
        Set<Integer> set = new HashSet<>();
        boolean falloBool = true;
        do {
            do {
                for (int i = 0; i < v1.length; i++) {
                    v1[i] = (int) (Math.random() * 49 + 1);
                }

                for(int num : v1){
                    falloBool = set.add(num);
                }

                if(!falloBool) {
                    System.out.println("\nEl boleto Nº " + cont + " ha sufrido una repetición y ha de repetirse el reparto de números");
                    System.out.println("\nEl boleto FALLIDO Nº " + cont + " es: ");
                    Arrays.sort(v1);
                    System.out.println(Arrays.toString(v1));
                }
            } while (!falloBool);

            System.out.println("\nEl boleto Nº " + cont + " es: ");
            Arrays.sort(v1);
            System.out.println(Arrays.toString(v1));
            cont++;
        } while (cont != 5);

    }
}