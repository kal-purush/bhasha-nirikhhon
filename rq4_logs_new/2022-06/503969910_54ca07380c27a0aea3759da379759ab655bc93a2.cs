using System;
using System.Globalization;

namespace Desafio_IMC
{
    internal class Program
    {
        static void Main(string[] args)
        {
            // double imc;
            string nome = "";
            string sexo = "";
            bool sexoEhValido = false;
            int idade;

            #region Cabeçalho do programa
            Console.WriteLine("**************************");
            Console.WriteLine("*** DIAGNÓSTICO PRÉVIO ***");
            Console.WriteLine("**************************");
            #endregion

            //Entrada de nome do usuário
            do
            {
                Console.Write("\nInsira seu nome completo: ");
                nome = Console.ReadLine();
            } while (string.IsNullOrWhiteSpace(nome)); //Valida a entrada de nome do usuário caso ele deixe o espaço em branco

            //Entrada do sexo do usuário
            do
            {
                Console.Write("\nInforme seu sexo, 'M' para Masculino ou 'F' para Feminino: ");
                sexo = Console.ReadLine();
                if (sexo == "m" || sexo == "M")
                {
                    sexo = "Masculino";
                    sexoEhValido = true;
                }
                else if (sexo == "f" || sexo == "F")
                {
                    sexo = "Feminino";
                    sexoEhValido = true;
                }
                else Console.WriteLine("\nValor inválido!\nDigite M para Masculino ou F para feminino");
            } while (!sexoEhValido); //Valida o valor de entrada do sexo do usuário caso ele informe outros caracteres além de 'f' ou 'm'

            //Faz a chamada do método ['VerificaIdade'] para o método ['Main'] fazendo com que a mensagem abaixo apareça para o usuário 
            idade = VerificaIdade("\nInforme sua idade: ");

            //Console.WriteLine("Nome: "+nome);

            /*Console.Write("\nDigite sua altura em metros: ");
            double altura = double.Parse(Console.ReadLine());

            Console.Write("\nInforme seu peso em kg: ");
            double peso = double.Parse(Console.ReadLine());

            imc = peso / Math.Pow(altura,2);*/

            /*if (idade >= 0 && idade < 12)
                Console.WriteLine("Categoria: Infantil");
            else if (idade >= 12 && idade <= 20)
                Console.WriteLine("Categoria: Juvenil");
            else if (idade >= 21 && idade <= 65)
                Console.WriteLine("Categoria: Adulto");
            else if (idade > 65 && idade < 123)
                Console.WriteLine("Categoria: Idoso");
            else Console.WriteLine("Valor inválido");*/

        }
        static int VerificaIdade(string msg) //Método criado para ler a idade do usuário e verificar se é um valor válido
        {
            bool idadeEhvalida = false;
            int ValidaIdadeInformada = 0;
            do
            {
                Console.Write(msg); //Mostra a mensagem no console pedindo para o usuário informar a idade
                var valorInformado = Console.ReadLine(); //Faz a leitura da idade informada pelo usuário                
                idadeEhvalida = int.TryParse(valorInformado, out ValidaIdadeInformada); //Valida a entrada do usuário verificando se o número que ele digitou é um valor válido referente a idade                
                if (!idadeEhvalida || ValidaIdadeInformada < 0)
                    Console.WriteLine("Valor inválido!\nInforme sua idade em anos, sem pontos ou vírgulas");
            } while (!idadeEhvalida || ValidaIdadeInformada < 0);
            return ValidaIdadeInformada;
        }
    }
}
