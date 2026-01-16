/*NOMES: Thômas e Daniel
TURMA: 302 INFO

COMBATE: Servirá para a janela em que ocorrerá o combate no jogo. 
Nesta classe encontra-se os principais métodos e atributos que envolverão o combate entre o usuário (user) e a máquina
 */
package controle;

import entidades.Chutes;
import entidades.Especiais;
import entidades.Personagem;
import entidades.Socos;
import java.util.Objects;

public class Combate {

    //Atributo da vida do user
    private int vidaUser;
    //Atributo da defesa do user
    private boolean userDefendendo;
    //Atributo da vida do adversário
    private int vidaAdversario;
    //Atributo da defesa do adversário
    private boolean adversarioDefendendo;
    //Atributo da energia do usuário
    private int energia;
    //Atributo da possibilidade do user defender
    private boolean possivelDefender;
    //Atributo do histórico do user
    private Personagem personagem;
    //Atributo de combate finalizada
    private boolean combateFinalizado;

    /*Construtor que inicaliza o Combate com a vida do user e do adversário em 100,
    a defesa do user e do adversário no false (desligadas),
    a possibilidade do user defender no true (ligada),
    o "combateFinalizado" igual a false
    e, por meio do Personagem passado por parâmetro, é verificado se o Personagem é aposentado (Se for, ele não luta)
     */
    public Combate(Personagem personagem) {
        Objects.requireNonNull(personagem, "O Objeto 'Personagem' não pode ser nulo");
        if (personagem.isAposentado()) {
            throw new IllegalArgumentException("Personagem aposentado não pode lutar");
        }
        this.personagem = personagem;
        this.vidaUser = 100;
        this.vidaAdversario = 100;
        this.adversarioDefendendo = false;
        this.energia = 7;
        this.possivelDefender = true;
        this.userDefendendo = false;
        this.personagem.getHistorico().partidaJogada();
        this.combateFinalizado = false;
    }

    //Getter da vida do user
    public int getVidaUser() {
        return this.vidaUser;
    }

    //Getter da vida do adversário
    public int getVidaAdversario() {
        return this.vidaAdversario;
    }

    //Getter da energia do user
    public int getEnergia() {
        return energia;
    }

    //Getter para verificar se o combate já foi finalizado
    public boolean isCombateFinalizado() {
        return combateFinalizado;
    }

    //Getter para verificar se o user está defendendo
    public boolean isUserDefendendo() {
        return userDefendendo;
    }

    //Getter para verificar se o adversário está defendendo
    public boolean isAdversarioDefendendo() {
        return adversarioDefendendo;
    }

    //Getter para verificar se é possivel o user defender
    public boolean isPossivelDefender() {
        return possivelDefender;
    }

    //Getter do Personagem do user
    public Personagem getPersonagem() {
        return personagem;
    }

    //Método que subtrai a vida do user
    public void danoAoUser(int vidaUser) {
        if (this.vidaUser - vidaUser >= 0) {
            this.vidaUser -= vidaUser;
        } else {
            this.vidaUser = 0;
        }
    }

    //Método que subtrai a vida do adversário
    public void danoAoAdversario(int vidaAdversario) {
        if (this.vidaAdversario - vidaAdversario >= 0) {
            this.vidaAdversario -= vidaAdversario;
        } else {
            this.vidaAdversario = 0;
        }
    }

    //Método que subtrai a energia do user
    public void perdaDeEnergia(int energia) {
        if (this.energia - energia >= 0) {
            this.energia -= energia;
        } else {
            this.energia = 0;
        }
    }

    /*Método que seleciona um golpe de mão (referente ao Enum Golpes_Maos)
    por meio do parâmetro selecionado.O usuário passa como parãmetro um valor inteiro
    entre 1 e 5, sendo cada valor referente a um ataque.
     */
    public String golpesMaosDoUser(int codGolpe) {

        //Se a partida já acabou, é retornada esta mensagem
        if (isCombateFinalizado()) {
            return "A PARTIDA JÁ FOI ENCERRADA";
        }

        //Percorre todos os golpes existentes até encontrar o do user
        for (Socos golpeEscolhido : Socos.values()) {

            //Verifica se o codigo passado é igual ao código do golpe
            if (codGolpe == golpeEscolhido.getCod()) {

                //Verifica se o custo energetico do golpe não é maior do que a energia do user
                if (this.getEnergia() - golpeEscolhido.getCusto() >= 0) {
                    this.possivelDefender = false;
                    //Subtrai a energia do user pelo custo energético do golpe
                    this.perdaDeEnergia(golpeEscolhido.getCusto());

                    //Verifica se o golpe será realizado com base em sua eficácia
                    if (Math.random() * 100 <= golpeEscolhido.getEficacia()) {

                        //Variável para o dano;
                        int dano = golpeEscolhido.getDano();

                        

                        //Verifica se o adversário está defendendo
                        if (isAdversarioDefendendo() == true) {

                            //Se está defendendo o dano é 1/3 dele
                            this.danoAoAdversario(dano / 3);

                            //Se a vida do adversário é 0, o user ganhou e retorna uma mensagem
                            if (this.getVidaAdversario() == 0) {
                                //É adicionado uma partida ganha para o usuário
                                this.getPersonagem().getHistorico().partidaGanha();
                                this.combateFinalizado = true;
                                return "VOCÊ GANHOU A LUTA";
                            } //Se o adversário não está com 0 de vida, retorna uma mensagem
                            else {
                                return "VOCÊ INFLINGIU " + dano / 3 + " DE DANO NO ADVERSÁRIO";
                            }
                        } //Se o adversário não está defendendo, o dano é por inteiro
                        else {

                            //Subtrai a vida do adversário pelo dano inteiro
                            this.danoAoAdversario(dano);

                            //Se a vida do adversário é 0, o user ganhou e retorna uma mensagem
                            if (this.getVidaAdversario() == 0) {
                                //É adicionada uma partida ganha ao usuário
                                this.getPersonagem().getHistorico().partidaGanha();
                                this.combateFinalizado = true;
                                return "VOCÊ GANHOU A LUTA";
                            } //Se o adversário não está com 0 de vida, retorna uma mensagem
                            else {
                                return "VOCÊ INFLINGIU " + dano + " DE DANO NO ADVERSÁRIO";
                            }
                        }
                    } //Se o golpe não foi eficaz, o user errou o golpe e retorna uma mensagem
                    else {
                        return "VOCÊ ERROU O ATAQUE :(";
                    }
                } //Se não energia suficiente para o golpe retorna uma mensagem
                else {
                    return "SEM ENERGIA SUFICIENTE PARA REALIZAR O GOLPE";
                }
            }
        }

        //Se o golpe não é achado, dá erro
        throw new IllegalArgumentException("Código de golpe inválido");
    }

    /*Método que seleciona um golpe de pé (referente ao Enum Golpes_Pes)
    por meio do parâmetro selecionado. O usuário passa como parãmetro um valor inteiro
    entre 1 e 5, sendo cada valor referente a um ataque.
    *OBS: A lógica interna e o passo a passo deste método é igual ao anterior.
     */
    public String golpePesDoUser(int codGolpe) {

        //Se a partida já acabou, é retornada esta mensagem
        if (isCombateFinalizado()) {
            return "A PARTIDA JÁ FOI ENCERRADA";
        }

        for (Chutes golpeEscolhido : Chutes.values()) {
            if (codGolpe == golpeEscolhido.getCod()) {
                if (this.getEnergia() - golpeEscolhido.getCusto() >= 0) {
                    this.perdaDeEnergia(golpeEscolhido.getCusto());
                    this.possivelDefender = false;
                    if (Math.random() * 100 <= golpeEscolhido.getEficacia()) {
                        int dano = golpeEscolhido.getDano();

                        

                        if (isAdversarioDefendendo() == true) {
                            this.danoAoAdversario(dano / 3);

                            if (this.getVidaAdversario() == 0) {
                                this.getPersonagem().getHistorico().partidaGanha();
                                this.combateFinalizado = true;
                                return "VOCÊ GANHOU A LUTA";
                            } else {
                                return "VOCÊ INFLINGIU " + dano / 3 + " DE DANO NO ADVERSÁRIO";
                            }
                        } else {
                            this.danoAoAdversario(dano);

                            if (this.getVidaAdversario() == 0) {
                                this.getPersonagem().getHistorico().partidaGanha();
                                this.combateFinalizado = true;
                                return "VOCÊ GANHOU A LUTA";
                            } else {
                                return "VOCÊ INFLINGIU " + dano + " DE DANO NO ADVERSÁRIO";
                            }
                        }
                    } else {
                        return "VOCÊ ERROU O ATAQUE :(";
                    }
                } else {
                    return "SEM ENERGIA SUFICIENTE PARA REALIZAR O GOLPE";
                }
            }
        }

        throw new IllegalArgumentException("Código de golpe inválido");
    }

    /*Método que seleciona um especial (referente ao Enum Especiais)
    por meio do parâmetro selecionado. O usuário passa como parãmetro um valor inteiro
    entre 1 e 5, sendo cada valor referente a um ataque.
    *OBS: A lógica interna e o passo a passo deste método é igual ao anterior.
     */
    public String EspeciaisDoUser(int codGolpe) {

        //Se a partida já acabou, é retornada esta mensagem
        if (isCombateFinalizado()) {
            return "A PARTIDA JÁ FOI ENCERRADA";
        }

        for (Especiais golpeEscolhido : Especiais.values()) {
            if (codGolpe == golpeEscolhido.getCod()) {
                if (this.getEnergia() - golpeEscolhido.getCusto() >= 0) {
                    this.perdaDeEnergia(golpeEscolhido.getCusto());
                    this.possivelDefender = false;
                    if (Math.random() * 100 <= golpeEscolhido.getEficacia()) {
                        int dano = golpeEscolhido.getDano();

                        if (isAdversarioDefendendo() == true) {
                            this.danoAoAdversario(dano / 3);

                            if (this.getVidaAdversario() == 0) {
                                this.getPersonagem().getHistorico().partidaGanha();
                                this.combateFinalizado = true;
                                return "VOCÊ GANHOU A LUTA";
                            } else {
                                return "VOCÊ INFLINGIU " + dano / 3 + " DE DANO NO ADVERSÁRIO";
                            }
                        } else {
                            this.danoAoAdversario(dano);

                            if (this.getVidaAdversario() == 0) {
                                this.getPersonagem().getHistorico().partidaGanha();
                                this.combateFinalizado = true;
                                return "VOCÊ GANHOU A LUTA";
                            } else {
                                return "VOCÊ INFLINGIU " + dano + " DE DANO NO ADVERSÁRIO";
                            }
                        }
                    } else {
                        return "VOCÊ ERROU O ATAQUE :(";
                    }
                } else {
                    return "SEM ENERGIA SUFICIENTE PARA REALIZAR O GOLPE";
                }
            }
        }

        throw new IllegalArgumentException("Código de golpe inválido");
    }

    //Método que ativa a defesa do user
    public String defender() {

        //O user ao defender, ativa a defesa, perde 3 de energia e pula o turno
        if (this.getEnergia() - 3 >= 0 && this.isPossivelDefender() == true) {

            this.perdaDeEnergia(3);
            this.userDefendendo = true;
            String msg = this.pularTurno();

            if (msg.equals("O ADVERSÁRIO ESTÁ DEFENDENDO")) {
                return "VOCÊ DEFENDEU, MAS O ADVERSÁRIO NÃO ATACOU E ESTÁ DEFENDENDO";
            } else if (msg.equals("VOCÊ PERDEU A LUTA")) {
                return msg;
            } else {
                return "VOCÊ DEFENDEU E O " + msg;
            }
        } else if (this.possivelDefender == false) {
            return "NÃO É POSSÍVEL DEFENDER. A DEFESA DEVE SER REALIZADA NO INÍCIO DO TURNO";
        } else if (this.getEnergia() - 3 < 0) {
            return "ENERGIA INSUFICIENTE PARA REALIZAR O ATAQUE";
        } else {
            return "NÃO FOI POSSÍVEL REALIZAR O ATAQUE";
        }

    }

    //Método que pulao turno do user para o adversário
    public String pularTurno() {

        //Se a partida já acabou, é retornada esta mensagem
        if (isCombateFinalizado()) {
            return "A PARTIDA JÁ FOI ENCERRADA";
        }

        this.energia += 7;

        //Verificando se o adversário irá defender
        if (Math.random() * 100 <= 30) {
            this.adversarioDefendendo = true;
            this.possivelDefender = true;
            this.userDefendendo = false;
            return "ADVERSÁRIO ESTÁ DEFENDENDO!";
        } //Se não ele irá efetuar possíveis três ataques
        else {

            int vidaIncial = this.getVidaUser();

            //DANO FRACO: Probabilidade alta do adversário acertar o golpe fraco
            if (Math.random() * 100 <= 90) {
                double danoDouble = Math.random() * 5;
                int danoInt = (int) danoDouble;

                if (this.isUserDefendendo() == true) {
                    this.danoAoUser(danoInt / 3);
                } else {
                    this.danoAoUser(danoInt);
                }
            }

            //DANO MÉDIO: Probabilidade média do adversário acertar o golpe médio
            if (Math.random() * 100 <= 60) {
                double danoDouble = (Math.random() * 5) + 5;
                int danoInt = (int) danoDouble;

                if (this.isUserDefendendo() == true) {
                    this.danoAoUser(danoInt / 3);
                } else {
                    this.danoAoUser(danoInt);
                }
            }

            //DANO FORTE: Probabilidade baixa do adversário acertar o golpe forte
            if (Math.random() * 100 <= 20) {
                double danoDouble = (Math.random() * 10) + 15;
                int danoInt = (int) danoDouble;

                if (this.isUserDefendendo() == true) {
                    this.danoAoUser(danoInt / 3);
                } else {
                    this.danoAoUser(danoInt);
                }
            }

            //Deixando o user novamente com a defesa desativada, mas com a possibilidade de defender
            this.possivelDefender = true;
            this.userDefendendo = false;

            //Após atacar o adversário não está defendendo
            this.adversarioDefendendo = false;

            //Verificando quanto de dano foi efetuado pelo adversário
            int danoFinal = vidaIncial - this.getVidaUser();

            //Se o user ficar com 0 de vida, ele perdeu e retorna a mensagem abaixo
            if (this.getVidaUser() == 0) {

                //É contabilizado no histórico do user 1 partida perdida
                this.getPersonagem().getHistorico().partidaPerdida();
                this.combateFinalizado = true;
                return "VOCÊ PERDEU A LUTA";

            } //Se o user ficar com a vida maior que 0, ele continua lutando e retorna a mensagem abaixo
            if (danoFinal == 0) {
                return "ADVERSÁRIO ERRA O ATAQUE";
            } else if (danoFinal >= 30) {
                return "ADVERSÁRIO ACERTA UM ATAQUE PODEROSO DE " + danoFinal + " DE DANO";
            }
            {
                return "ADVERSÁRIO INFLINGIU " + danoFinal + " DE DANO!";
            }
        }
    }
}