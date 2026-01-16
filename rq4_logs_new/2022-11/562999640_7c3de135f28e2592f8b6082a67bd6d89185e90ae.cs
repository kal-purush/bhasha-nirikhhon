using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Trilha.Model;
using Trilha.Models;

namespace Trilha.Controllers;

public class JogoController
{
    private readonly JogadorModel JogadorVermelho;
    private readonly JogadorModel JogadorAzul;
    private readonly TabuleiroModel Tabuleiro;


    /// <summary>
    /// Construtor que recebe os parametros procurados no banco de dados para inicializar os jogadores
    /// </summary>
    /// <param name="idVermehlo">Id do jogador vermelho</param>
    /// <param name="nomeVermelho">Nome do jogador Vermelho</param>
    /// <param name="vitoriasVermelho">Quantidade de vitórias do jogador Vermelho</param>
    /// <param name="idAzul">Id do jogador Azul</param>
    /// <param name="nomeAzul">Nome do jogador Azul</param>
    /// <param name="vitoriasAzul">Quantidade de vitórias do jogador Azul</param>
    public JogoController(int idVermehlo, String nomeVermelho, int vitoriasVermelho, int idAzul, String nomeAzul, int vitoriasAzul)
    {
        JogadorVermelho = new JogadorModel(idVermehlo, nomeVermelho, vitoriasVermelho, 0, CorJogador.Vermelho);
        JogadorAzul = new JogadorModel(idAzul, nomeAzul, vitoriasAzul, 0, CorJogador.Azul);

        Tabuleiro = new TabuleiroModel();
    }


    /// <summary>
    /// Posiciona as peças no tabuleiro
    /// </summary>
    /// <param name="cor">Indica a cor do jogador</param>
    /// <param name="posicao">Posição do vetor que a peça deve ser posicionada</param>
    /// <returns>Retorna a Cor do jogador que realizará a próxima jogada</returns>
    public CorJogador Jogar(CorJogador cor, int posicao)
    {
        if (VerificaPecasPosicionamento(cor))
            return cor.Equals(CorJogador.Azul) ? CorJogador.Vermelho : CorJogador.Azul;
        
        var proximo = Tabuleiro.PosicionaPeca(cor, posicao);

        if (NotEquals(cor, proximo))
            IncrementaPecas(cor);

        return proximo;
    }


    /// <summary>
    /// Assim que todos os jogadores possuem 9 peças começa a movimentação de peças
    /// </summary>
    /// <param name="cor">Indica a cor do jogador</param>
    /// <param name="origem">Posição de origem do vetor que a peça deve ser Movida</param>
    /// <param name="destino">Posição de Destino do vetor que a peça deve ser Posicionada</param>
    /// <returns>Retorna a Cor do jogador que realizará a próxima jogada</returns>
    public CorJogador Jogar(CorJogador cor, int origem, int destino)
    {

        return Tabuleiro.MovimentaPeca(cor, origem, destino);
    }


    /// <summary>
    /// Soma a quantidade de pecas do jogador
    /// </summary>
    /// <param name="cor">Indica qual jogador deve ter sua pessa somada</param>
    private void IncrementaPecas(CorJogador cor)
    {
        if (cor.Equals(CorJogador.Vermelho))
            JogadorVermelho.QuantidadePecas++;

        else
            JogadorAzul.QuantidadePecas++;
    }


    /// <summary>
    /// Verifica se os 2 jogadores possuem até 9 peças
    /// </summary>
    /// <returns>Se O jogador em questão pode realizar o posicionamento de peças</returns>
    private bool VerificaPecasPosicionamento(CorJogador cor) => cor.Equals(CorJogador.Vermelho)
    ? JogadorVermelho.QuantidadePecas >= 9
    : JogadorAzul.QuantidadePecas >= 9;


    /// <summary>
    /// Verifica se as duas cores são diferentes
    /// </summary>
    /// <param name="atual">A cor do player que acabou de jogar</param>
    /// <param name="proximo">A cor do próximo player que irá jogar</param>
    /// <returns>Se os parametros são diferentes</returns>
    private bool NotEquals(CorJogador atual, CorJogador proximo)
    {
        if (atual.Equals(proximo))
            return false;
        else
            return true;
    }
}
