using ProjetoCozinheiro.Componentes;
using ProjetoCozinheiro.MiniJogos;

namespace ProjetoCozinheiro.Cenas;

public class FaseFugu : FaseBase
{
    public FaseFugu(int reputacaoExigida, int pontuacaoVitoria) : base(reputacaoExigida, pontuacaoVitoria)
    {
        Descricao = $"Okimataro Atiro (Japão) [Reputação necessária: {reputacaoExigida}]";
    }

    public override void Executar()
    {
        Introducao();
        (Pontuacao, var reacao) = Pergunta1();

        if (Pontuacao <= 0)
        {
            Derrota();
        }
        else
        {
            Pontuacao += Pergunta2(reacao);
            var miniJogo = Pergunta3();
            var p = miniJogo.Executar(Math.Abs(100 - Pontuacao), false);

            if (p <= 0)
            {
                Derrota();
            }
            else
            {
                Pontuacao += p * 5;
                Pontuacao += Pergunta4();
                Pontuacao += Pergunta5();

                if (Pontuacao > PontuacaoVitoria)
                {
                    Vitoria();
                }
                else
                {
                    Derrota();
                }
            }
        }
        
    }
    
    private void Introducao()
    {
        var menu = new Menu<OpcoesMenu>("Okimataro Atiro (Japão)",
                        $"{Imagens.AVIAO}\nCriar uma pequena introdução sobre o chefe que será enfrentado!",
                        new Dictionary<string, OpcoesMenu>
                        {
                                        {"Continuar", OpcoesMenu.Sair}
                        }
        );

        _ = menu.Mostrar();
    }
    
    (int, string) Pergunta1()
    {
        var menu = new Menu<(int, string)>("Okimataro Atiro (Japão)",
                        @"Konichiwaaaaaaaaaaaa, já pensou onde estamos? isso mesmo, em 
                               Osaka no JAPÂO! Já imaginou o prato que irá preparar? Não!
                               Uma dica, pode ser MORTAL! O nome do prato é FUGU, 
                               uma comida que exigirá habilidades extraordinárias!
                               Vamos começar escolhendo os ingredientes principais",
                        new Dictionary<string, (int, string)>
                        {
                                        {"Salmão, arroz e molho shoyu", (0, "https://www.youtube.com/watch?v=qwK9o9-Hk14")},
                                        {"Peixe-leão, limão e pimenta", (0, "https://www.youtube.com/watch?v=qwK9o9-Hk14")},
                                        {"Polvo de aneis azuis, alho e sal", (0, "https://www.youtube.com/watch?v=qwK9o9-Hk14")},
                                        {"Peixe-Baiacu, alho, limão e sal", (30, "Já temos os ingredientes necessários")},
                                        {"Contratar um chef em comidas perigosas", (0, "https://www.youtube.com/watch?v=qwK9o9-Hk14")},
                                        {"Tubarão branco e brasas de carvão acessas", (0, "https://www.youtube.com/watch?v=qwK9o9-Hk14")},
                                        {"Escopião, limão e molho de tomate", (0, "https://www.youtube.com/watch?v=qwK9o9-Hk14")},
                        }
        );

        return menu.Mostrar();
    }
    
    int Pergunta2(string reacao)
    {
        var menu = new Menu<int>("Okimataro Atiro (Japão)",
                        $"{reacao} Vamos lá, vamos começar a cortar o baiacu, se não for cortado corretamente você perde, qual o passo adiante que deve seguir?",
                        new Dictionary<string, int>
                        {
                                        {"Pegar seus equipamentos como faca de corte preciso", 30},
                                        {"Utilizar uma espada para cortar", 20},
                                        {"Utilizar um bisturi", 0},
                                        {"Faca tramontina", 15},
                        }
        );

        return menu.Mostrar();
    }
    
    MiniJogos.MiniJogos Pergunta3()
    {
        var menu = new Menu<MiniJogos.MiniJogos>("Okimataro Atiro (Japão)",
                        @"Chegamos a etapa do corte! Muito cuidado tem que acertar o corte no tempo limite e preciso, digite as teclas que vai aparecer na sua tela no tempo limite",
                        new Dictionary<string, MiniJogos.MiniJogos>
                        {
                                        {"Vamos", new MiniJogoDigitacao()}
                        });

        return menu.Mostrar()!;
    }
    
    int Pergunta4()
    {
        var menu = new Menu<int>("Okimataro Atiro (Japão)",
                        "Otimo, você cortou o baiacu corretamente, está pronto seu fugu, é hora de?",
                        new Dictionary<string, int>
                        {
                                        {"Apreciar com moderação", 35},
                                        {"Saborear com calma, caso não esteja cortado corretamente", 10},
                        });

        return menu.Mostrar();
    }

    int Pergunta5()
    {
        var menu = new Menu<int>("Okimataro Atiro (Japão)",
                        "Seu molho está pronta, é hora de?",
                        new Dictionary<string, int>
                        {
                                        {"Cortar os pão e colocar a salsicha com o molho", 15},
                                        {"Deixar esfriar um pouco e deixar o molho na panela", 5},
                        });

        return menu.Mostrar();
    }
    
    private void Sair(string reacao)
    {
        var menu = new Menu<int>("Okimataro Atiro (Japão)",
                        $"Péssimo! Clique no link, {reacao}",
                        new Dictionary<string, int>
                        {
                                        {"Voltar perdedor!", 0}
                        }
        );

        _ = menu.Mostrar();
    }
}