using System;
using System.Collections.Generic;
using System.Text;

namespace ExercicioVetores
{
    class Hospedagem
    {
        //ATRIBUTOS
        public string Nome { get; set; }
        public string Email { get; set; }
        public int Quarto { get; set; }

        //CONSTRUTOR
        public Hospedagem(string nome, string email, int quarto)
        {
            Nome = nome;
            Email = email;
            Quarto = quarto;
        }

        public Hospedagem()
        {
        }

        public override string ToString()
        {
            return
                +Quarto + ": " + Nome + ", " + Email;
        }
    }
}