using System;

namespace Aula_10_26_2023_Ex_doWhile_While
{
    class Program
    {
        static void Main(string[] args)
        {
            float valorFinanciado, valorParcela;
            int parcela, contador = 1;

            Console.Write("Informe o valor financiado: ");
            valorFinanciado = float.Parse(Console.ReadLine());

            Console.Write("Informe o número de parcelas: ");
            parcela = int.Parse(Console.ReadLine());

            valorParcela = valorFinanciado / parcela;

            DateTime data = DateTime.Now.Date; // DateTime armazena uma data e um horário-

            while (contador <= parcela)
            {
                //Console.WriteLine($"Parcela n° {contador}, valor = {valorFinanciado / parcela}");
                //Console.WriteLine($"{contador}ª parcela: {valorFinanciado / parcela:0.00}");
                Console.WriteLine($"{contador}ª parcela ({data}): {valorParcela}");
                contador++;
                data = data.AddMonths(1);
            }

            Console.WriteLine("ta pago"); // criar váriavel para realizar a conta no laço de repetição consome menos memória
        }                                 // Pois a conta será realizada apenas uma vez, já colocando no laço a conta será feita várias vezes    
    }
}