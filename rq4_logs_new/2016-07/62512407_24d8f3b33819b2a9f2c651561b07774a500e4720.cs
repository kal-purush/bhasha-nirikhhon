using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Exercicio01_Semana04
{
    /// <summary>
    /// Faça um programa que leia 8 variaveis e as passe como parametro para um construtor, esse construtor
    /// deverá colocar as variaveis no vetor. E depois calcular e retornar a Média das Notas. (Construtores não possuem tipo de retorno).
    /// </summary>

    class Semana04_Notas
    {
        static void Main(string[] args)
        {
            int n1 = 10;
            int n2 = 10;
            int n3 = 10;
            int n4 = 10;
            int n5 = 0;
            int n6 = 0;
            int n7 = 10;
            int n8 = 10;


            Vetor_Pasado oito = new Vetor_Pasado(n1,n2,n3,n4,n5,n6,n7,n8);
            var M = oito.Retorna_Media();
            Console.WriteLine(M);
            Console.ReadLine();
        }
    }
}