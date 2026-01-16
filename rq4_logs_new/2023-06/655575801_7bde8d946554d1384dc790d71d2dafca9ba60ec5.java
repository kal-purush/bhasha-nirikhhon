package AlgoritmosEstruturasdeDados.AlgoritmosPesquisa;

public class PesquisaBinaria {

    public static int PesquisaBinaria(int[] array, int valor) {
        int inicio = 0; // Define o índice mais baixo do intervalo de busca
        int fim = array.length - 1; // Define o índice mais alto do intervalo de busca

        while (inicio <= fim) { // Enquanto o intervalo de busca não se esgotar
            int meio = (inicio + fim) / 2; // Calcula o índice do elemento central

            if (array[meio] < valor) {
                inicio = meio + 1; // Se o elemento central for menor que a chave, o intervalo de busca se move para a direita do elemento central
            } else if (array[meio] > valor) {
                fim = meio- 1; // Se o elemento central for maior que a chave, o intervalo de busca se move para a esquerda do elemento central
            } else {
                return meio; // Se o elemento central for igual à chave, retorna o índice do elemento central
            }
        }

        return -1; // Se a chave não for encontrada no array, retorna -1
    }
}