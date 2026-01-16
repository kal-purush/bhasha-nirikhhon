using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Smartphone
{
    internal class Iphone : Smartphone
    {
        string nome;

        public void Ligar()
        {

            Console.WriteLine("Ligando usando meu Iphone....");
        }

        public void ReceberLigacao()
        {

            Console.WriteLine("Recebendo ligação no meu celular Iphone.....");
        }




        public override void InstalarAplicativo()
        {
            Console.WriteLine("escreva o nome do app que deseja instalar...");
            nome = Console.ReadLine();
            Console.WriteLine("instalando.....por favor espere...");


            Console.WriteLine("o app " + nome + " foi instalado com sucesso no seu Iphone!");
        }
    }
}