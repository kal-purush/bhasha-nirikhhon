using HeroisDoCancer.Models;
using System;
using System.Collections.Generic;

namespace HeroisDoCancer.ContextoDados
{
    public class DadosEmMemoria : IContextoDados
    {
        private static readonly Object locker = new Object();
        private static Boolean iniciado;
        private static List<Voluntario> voluntarios;

        public DadosEmMemoria()
        {
            IniciarDados();
        }

        public ICollection<Voluntario> Voluntarios
        {
            get { return voluntarios; }
        }

        public void Adiciona<TModel>(TModel model) where TModel : class
        {
            lock (locker)
            {
                ObterModelosPelo<TModel>().Add(model);
            }
        }

        public void Remove<TModel>(TModel model) where TModel : class
        {
            lock (locker)
            {
                ObterModelosPelo<TModel>().Remove(model);
            }
        }

        public void Publica() { }

        private static List<TModel> ObterModelosPelo<TModel>()
        {
            var tipo = typeof(TModel);
            if (tipo == typeof(Voluntario))
            {
                return voluntarios as List<TModel>;
            }

            throw new Exception("Dados não implementados para o tipo " + tipo.Name);
        }

        private static void IniciarDados()
        {
            if (!iniciado)
            {
                lock (locker)
                {
                    if (!iniciado)
                    {
                        PopularDados();
                        iniciado = true;
                    }
                }
            }
        }

        private static void PopularDados()
        {
            voluntarios = new List<Voluntario> 
            {
                new Voluntario 
                {
                    Id = 1,
                    Nome = "Guilherme Britz Videira",
                    Email = "gbritzv@gmail.com",
                    Eventos = new List<Evento>(),
                    Foto = String.Empty,
                    Senha = "123456"
                }            
            };
        }
    }
}