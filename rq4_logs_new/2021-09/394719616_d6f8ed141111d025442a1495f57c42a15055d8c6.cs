using System;
using System.Collections.Generic;
using System.Text;

namespace ExercicioFixacaoVetores
{
    class Colaborador
    {
        // ATRIBUTOS
        public int Id { get; set; }
        public string Nome { get; set; }
        public double Salario { get; set; }

        // CONSTRUTORES
        public Colaborador (int id, string nome, double salario)
        {
            Id = id;
            Nome = nome;
            Salario = salario;
        }

        public Colaborador (int id, string nome)
        {
            Id = id;
            Nome = nome;
        }

        public Colaborador()
        {}

        // MÉTODOS
        public double aumentoSalario(int idAum, double aumento)
        {
            double percentual = aumento / 100;
            double percAumento = Salario * percentual;
            double novoSalario = Salario + percAumento;
            Salario = novoSalario;

            return novoSalario;
        }

        public override string ToString()
        {
            return
                Id + ", " + Nome + ", " + Salario;
        }

    }
}