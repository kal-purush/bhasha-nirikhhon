using SuperERP.DAL.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SuperERP.DAL.Repositories
{
    public class CategoriaRepositorio : Repositorio<Categoria>
    {
        //Categoria usa tipo boooleano
        //  True é serviço
        //  False é produto
        public ICollection<Categoria> PegarTodasCategorias(Boolean tipo)
        {
            var categorias = dbContext.Categorias.Where(c => c.CategoriaTipo == tipo);
            return categorias.ToList();
        }
    }
}