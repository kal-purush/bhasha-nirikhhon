/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package proj;

/**
 *
 * @author joaod
 */
import java.util.ArrayList;
import java.util.List;

public class Domino {
    private List<Jogador> jogadores;
    private Tabuleiro tabuleiro;
    private int jogadorAtual;
    
    public Domino(String nomeJogador1, String nomeJogador2) {
        jogadores = new ArrayList<>();
        jogadores.add(new Jogador(1, nomeJogador1));
        jogadores.add(new Jogador(2, nomeJogador2));
        
        tabuleiro = new Tabuleiro();
        jogadorAtual = 0;
    }
    
    public void jogarPeca(int idPeca, int lado) {
        Jogador jogador = jogadores.get(jogadorAtual);
        Peca peca = jogador.getPecaById(idPeca);
        
        if (peca != null && tabuleiro.podeJogarPeca(peca, lado)) {
            tabuleiro.jogarPeca(peca, lado);
            jogador.removerPeca(peca);
            
            // Troca para o próximo jogador
            jogadorAtual = (jogadorAtual + 1) % jogadores.size();
        }
    }
    
    // Outros métodos e lógica do jogo...
}