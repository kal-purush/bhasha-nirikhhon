using Gestao_de_Projetos.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Gestao_de_Projetos.Data
{
    public class SeedDataDadosFIT
    {
        internal static void Populate(Gestao_de_ProjetosContext bd)
        {
            PopulateClientes(bd);
            PopulateProject(bd);

        }

        private static void PopulateProject(Gestao_de_ProjetosContext bd)
        {
            if (bd.Project.Any()) return;
            bd.Project.AddRange(
                new Project
                {
                    Nome_projeto = "Remodelação exterior",
                    DataInicio = DateTime.Parse("12/12/2021"),
                    ClientesId = 1,
                    DataPrevistaFim = DateTime.Parse("12/26/2021")
                },

                 new Project
                 {
                     Nome_projeto = "Remodelação Interior",
                     DataInicio = DateTime.Parse("12/12/2021"),
                     ClientesId = 1,
                     DataPrevistaFim = DateTime.Parse("12/27/2021")
                 },
                  new Project
                  {
                      Nome_projeto = "Contrução do Jardim",
                      DataInicio = DateTime.Parse("12/12/2021"),
                      ClientesId = 1,
                      DataPrevistaFim = DateTime.Parse("12/30/2021")
                  },

                   new Project
                   {
                       Nome_projeto = "Remodelação exterior",
                       DataInicio = DateTime.Parse("12/12/2021"),
                       ClientesId = 1,
                       DataPrevistaFim = DateTime.Parse("12/30/2021")
                   }
                  );

        }

        private static void PopulateClientes(Gestao_de_ProjetosContext bd)
        {
            if (bd.Clientes.Any()) return;
            bd.Clientes.AddRange(
                new Clientes
                {
                    Nome = "José",
                    Email = "joseraf@gmail.com",
                    Contacto = "931184481",
                    NIF = "289761182",

                },

                   new Clientes
                   {
                       Nome = "José",
                       Email = "joseraf@gmail.com",
                       Contacto = "931184481",
                       NIF = "289761182",

                   },

                     new Clientes
                     {
                         Nome = "José",
                         Email = "joseraf@gmail.com",
                         Contacto = "931184481",
                         NIF = "289761182",

                     },


                     new Clientes
                     {
                         Nome = "José",
                         Email = "joseraf@gmail.com",
                         Contacto = "931184481",
                         NIF = "289761182",

                     }
                     );
            bd.SaveChanges();


        }

    }
}