using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ProjetoAtendimento.Models;

namespace ProjetoAtendimento.Controllers
{
    class Guiches
    {
        #region atributos
        private List<Guiche> listaGuiches;
        #endregion

        #region propriedades
        public List<Guiche> ListaGuiches { get { return listaGuiches; } } 
        #endregion

        #region construtores
        public Guiches() {
            listaGuiches = new List<Guiche>();
        }
        #endregion

        #region metodos
        public void adicionar(Guiche guiche)
        {
            listaGuiches.Add(new Guiche(listaGuiches.Count + 1));
        }
        #endregion
    }
}