using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TesteFormulas
{
    class Trocas
    {
        public static void realizarTrocas(GuichesSetup[] guiches, Queue<Pessoas>[] fila, int troca)
        {
            int maiorPeso = 0;  //essa variavel serve para achar o maior peso atualmente
            int posicaoMaior = -1;

            int menorPeso = Constantes.infinito;    //essa variavel serve pra achar o menor peso atualmente
            int posicaoMenor = -1;


            atualizarPesos_Atendentes(guiches, fila, troca);

            //area de formulas
            //ainda nao tem rs



            for (int i = 0; i < guiches.Length; i++)
            {
                if (guiches[i].atendente == false && guiches[i].chegadaAtendente == 0 && guiches[i].peso > maiorPeso)//achando o maior peso (sem atendente e nem com atendente a caminho) e guardando ele e a posicao
                {
                    maiorPeso = guiches[i].peso;
                    posicaoMaior = i;
                }
                if (guiches[i].atendente == true && guiches[i].vazio == true && guiches[i].peso < menorPeso)//achando o menor peso (com atendente) e guardando ele e a posicao
                {
                    menorPeso = guiches[i].peso;
                    posicaoMenor = i;
                }
            }
            if (maiorPeso > menorPeso + troca)
            {
                trocarAtendentes(guiches, posicaoMenor, posicaoMaior); //se o maiorPeso for maior que o menorPeso + a troca * 3, então, efetuará a troca
            }
        }

        static void trocarAtendentes(GuichesSetup[] guiches, int guiche1, int guiche2)
        {   //essa funcao faz troca de atendentes, da primeira posicao para a 2 posicao
            guiches[guiche1].atendente = false;
            guiches[guiche2].chegadaAtendente = 1;
        }
        static void atualizarPesos_Atendentes(GuichesSetup[] guiches, Queue<Pessoas>[] fila, int troca)
        {
            int j;
            for (int i = 0; i < guiches.Length; i++)
            {
                //isso serve para o guiche achar a fila referente a ele (a fila de guiches iguais tem o mesmo "numero" do 1 guiche)
                j = i;
                while (j != 0 && guiches[j].guiche == guiches[j - 1].guiche) j--;
                //esse peso inicialmente atribuido é o tamanho da fila * turnos necessarios para atender um cliente, logo é a quantidade de turnos para a fila esvaziar
                guiches[i].peso = fila[j].Count * guiches[i].turnosNecessarios;

                if (guiches[i].chegadaAtendente != 0)   //verificando se tem algum atendente indo a esse guiche
                    if (guiches[i].chegadaAtendente >= troca)   //verificando se já deu o tempo de troca
                    {
                        guiches[i].atendente = true;
                        guiches[i].chegadaAtendente = 0;
                    }
                    else guiches[i].chegadaAtendente++; //caso ainda não deu o tempo de troca, somente incrementa o tempo e segue o jogo
            }
        }
    }
}