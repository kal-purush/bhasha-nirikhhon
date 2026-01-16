import data.DataPointSimple;
import util.DataReader;
import util.FileBuilder;
import util.HtmlBuilder;
import util.Tools;

import java.util.ArrayList;

/**
 *
 *  @author André Gaeta <andremg@dcc.ufrj.br>
 *
 * Classe para facilitar a testagem e mostrar a execução dos programas da Lista III.
 *
 */

public class Main {
    public static void main(String[] args) {
        menuInicial();
    }

    private static void menuInicial(){
        System.out.println("Qual tipo de dado você deseja gerar?");
        System.out.println("1. Gráfico (HTML)");
        System.out.println("2. Ranking (arquivo)");


        int escolha = 0;
        escolha = Tools.getInt();
        while(escolha < 1 || escolha > 2){
            System.out.println("Escolha invalida. Digite um numero de 1 a 2.");
            escolha = Tools.getInt();
        }

        switch(escolha) {
            case 1:
                menuGrafico();
                break;
            case 2:
                menuArquivo();
                break;
        }
    }

    private static void menuGrafico(){
        System.out.println("Qual o local desejado?");
        System.out.println("1. Cidade");
        System.out.println("2. Estado");
        System.out.println("3. Geral");

        int escolha = 0;
        escolha = Tools.getInt();
        while(escolha < 1 || escolha > 3){
            System.out.println("Escolha invalida. Digite um numero de 1 a 3.");
            escolha = Tools.getInt();
        }

        switch(escolha) {
            case 1:
                System.out.println("Digite a cidade desejada.");
                String escolhaCidade =  Tools.getString();
                ArrayList<DataPointSimple> listCidade = DataReader.getSimpleDataByLocation(escolhaCidade, true);
                if(listCidade.isEmpty()){
                    System.out.println("Cidade não encontrada. Tente novamente.");
                }
                else{
                    HtmlBuilder.createHTML(listCidade);
                    System.out.println("HTML criado com sucesso.");
                }
                break;
            case 2:
                System.out.println("Digite o estado desejado.");
                String escolhaEstado = Tools.getString();
                ArrayList<DataPointSimple> listEstado = DataReader.getSimpleDataByLocation(escolhaEstado, false);
                if(listEstado.isEmpty()){
                    System.out.println("Cidade não encontrada. Tente novamente.");
                }
                else{
                    HtmlBuilder.createHTML(listEstado);
                    System.out.println("HTML criado com sucesso.");
                }
                break;
            case 3:
                HtmlBuilder.createHTML(DataReader.getSimpleDataTotal());
                System.out.println("HTML criado com sucesso.");
                break;
        }


        menuInicial();
    }

    private static void menuArquivo(){
        System.out.println("Qual o ranking desejado?");
        System.out.println("1. Maior número de casos por 100k habitantes.");
        System.out.println("2. Menor número de casos por 100k habitantes.");
        System.out.println("3. Maior taxa de mortalidade.");
        System.out.println("4. Menor taxa de mortalidade.");
        System.out.println("5. Maior taxa de crescimento de casos no último mês.");

        int escolha = 0;
        escolha = Tools.getInt();
        while(escolha < 1 || escolha > 5){
            System.out.println("Escolha invalida. Digite um numero de 1 a 5.");
            escolha = Tools.getInt();
        }

        switch (escolha){
            case 1:
                FileBuilder.createMaiorCasos100k();
                break;
            case 2:
                FileBuilder.createMenorCasos100k();
                break;
            case 3:
                FileBuilder.createMaiorMortalidade();
                break;
            case 4:
                FileBuilder.createMenorMortalidade();
                break;
            case 5:
                FileBuilder.createMaiorTaxaCrescimento();
                break;
        }
        System.out.println("Ranking criado com sucesso.");
        menuInicial();

    }
}