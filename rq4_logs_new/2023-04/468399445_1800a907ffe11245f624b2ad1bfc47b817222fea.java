import java.util.Scanner;

public class MyClass {
    public static void main(String args[]) {
        
      Scanner sc = new Scanner(System.in);

      // questão 1
      int indice, soma, k;
      indice = 13;
      soma = 0;
      
      for (k= 0; k < indice; k++) {
          soma += k;
            System.out.println(soma); // valor de soma no final = 78
      }
      
      //questão 2
      System.out.print("Digite um número: ");
      int n = sc.nextInt();
      
      int finboAnt, finboProx, finboAtual;
      finboAnt = 0;
      finboAtual = 1;
      
      while (finboAtual < n) {
          finboProx = finboAnt + finboAtual;
          finboAnt = finboAtual;
          finboAtual = finboProx;
      }
      
      if (finboAtual == n) {
          System.out.println("O número " + n + " pertence a sequência.");
      } else {
          System.out.println("O número " + n + " não pertence a sequência.");
      }
      
      sc.close();
      
      /* 
      questão 3
      a) 1, 3, 5, 7, 9... (adicionar sempre +2 ao número anterior)
      b) 2, 4, 8, 16, 32, 64, 128... (multiplicar o número anterior por 2)
      c) 0, 1, 4, 9, 16, 25, 36, 49... (adicionar números impares a dos quadradrados perfeitos)
      d) 4, 16, 36, 64, 100... (adicionar 12 a cada um na sequência)
      e) 1, 1, 2, 3, 5, 8, 13... (somar os dois últimos números pra saber o próximo)
      f) não consegui identificar
      
      questão 4
      100 - x {a distância que o carro percorre é x e a distância que o caminhão percorre é 100 - x}
      t = d/v {d = distância percorrida, v = velocidade}
      carro = x/110
      caminhao = (100 - x)/ 80 + 0,1667
      x/110 = (100 - x)/80 + 0,1667
      x = 40,91 km.
      ponto de encontro 40,91 km de Ribeirão Preto e 59,09 km de Franca
      carro precisa percorrer uma distância de 100 - x = 59,09 km até Ribeirão Preto.
      caminhão precisa percorrer uma distância de x = 40,91 km até Ribeirão Preto.
      caminhão está mais próximo de Ribeirão Preto
      
      
      */
      
      //questão 5
      
      String str = "Esta string vai ficar ao contrário";
      String strInvertida = "";

      for (int i = str.length() - 1; i >= 0; i--) {
            strInvertida += str.charAt(i);
      }

      System.out.println("Original: " + str);
      System.out.println("Invertida: " + strInvertida);
     
    }
}