/*
* Olá cenouras e cenoures!
*/
using System;

namespace Erebus_v3
{
	class Personagem
	{
		public string classe;
		public int escolha;
		public string name;
		public int life;
		public int def;
		public int atk;
	}
	
	class MainProgram
	{
		public static void Main(string[] args)
		{
			
			
			Personagem p1 = new Personagem();
			
			Console.WriteLine(" --------------------\n" + 
			                  "  Qual o seu nome? \n" + 
			                  " --------------------");
			p1.name = Console.ReadLine();
			Console.WriteLine(" -------------------------------------------\n" + 
			                  "        Escolha uma das classes:          \n" +
			                  "  1 - Guerreiro | 2 - Arqueiro | 3 - Mago \n" +
			                  " -------------------------------------------");
			p1.escolha = int.Parse(Console.ReadLine());
			Console.Clear();
			
			if (p1.escolha == 1){
				p1.classe = "Guerreiro";
				p1.life = 150;
				p1.def = 5;
				p1.atk = 10;
			}else if (p1.escolha == 2){
				p1.classe = "Arqueiro";
				p1.life = 150;
				p1.def = 10;
				p1.atk = 5;
			}else if(p1.escolha == 3){
				p1.classe = "Mago";
				p1.life = 100;
				p1.def = 10;
				p1.atk = 10;
			}
			
			Console.WriteLine (" --------------------------------------------------------------------\n" + 
			                   "      Nome: {0} | Classe: {1}                                      \n" + 
			                   "      Vida {2} | Defesa {3} | Ataque {4}                           \n" +
			                   " --------------------------------------------------------------------", p1.name, p1.classe, p1.life, p1.def, p1.atk);
			Console.ReadKey(true);
		}
	}
}