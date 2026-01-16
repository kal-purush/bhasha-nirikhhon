package grafo.navegacao.arvoreHeap;

import java.util.ArrayList;
import java.util.List;

public class ArvoreHeap {
    private final int RAIZ = 1;

    private List<FolhaHeap> arvore;

    public ArvoreHeap() {
        this.arvore = new ArrayList<>();
        this.arvore.add(null);
    }

    public void inserir(FolhaHeap folha) {
        arvore.add(folha);
        int posicaoAtual = getTamanho();

        while (getRaiz(posicaoAtual) > 0 && folha.getCusto() < arvore.get(getRaiz(posicaoAtual)).getCusto()) {
            troca(posicaoAtual, getRaiz(posicaoAtual));
            posicaoAtual = getRaiz(posicaoAtual);
        }
    }

    public FolhaHeap remover() {
        FolhaHeap folha = arvore.remove(RAIZ);
        if (getTamanho() > 1) {
            arvore.toArray()[RAIZ] = arvore.get(getTamanho());
            minHeapify(RAIZ);
        }

        return folha;
    }

    public int getTamanho() {
        return arvore.size() - 1;
    }

    private int getRaiz(int posicao) {
        return  posicao / 2;
    }

    private int getFolhaEsquerda(int posicao) {
        return posicao * 2;
    }

    private int getFolhaDireita(int posicao) {
        return (posicao * 2) + 1;
    }

    private boolean eFolha (int posicao) {
        int tamanhoArvore = getTamanho();
        return ((posicao >= tamanhoArvore / 2) && (posicao <= tamanhoArvore));
    }

    private void troca (int folha, int raizFolha) {
        FolhaHeap temp = arvore.get(folha);
        arvore.toArray()[folha] = arvore.get(raizFolha);
        arvore.toArray()[raizFolha] = temp;
    }

    private void minHeapify(int posicao) {
        if(!eFolha(posicao)) {
            FolhaHeap atual = arvore.get(posicao);
            FolhaHeap folhaEsquerda = arvore.get(getFolhaEsquerda(posicao));
            FolhaHeap folhaDireita = arvore.get(getFolhaDireita(posicao));

            if(atual.getCusto() > folhaEsquerda.getCusto() || atual.getCusto() > folhaDireita.getCusto()) {
                if (folhaEsquerda.getCusto() < folhaDireita.getCusto()) {
                    troca(posicao, getFolhaEsquerda(posicao));
                    minHeapify(getFolhaEsquerda(posicao));
                } else {
                    troca(posicao, getFolhaDireita(posicao));
                    minHeapify(getFolhaDireita(posicao));
                }
            }
        }
    }
}