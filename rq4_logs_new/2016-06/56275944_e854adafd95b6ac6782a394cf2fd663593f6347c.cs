using SuperERP.DAL.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SuperERP.DAL.Repositories
{
    public class ContatoRepository : Repositorio<Contato>
    {
        public Contato ObterPorPessoaFisica(int id)
        {
            return dbContext.Contatos.Where(x => x.IdPessoaFisica == id).FirstOrDefault();
        }
        public Contato ObterPorPessoaJuridica(int id)
        {
            return dbContext.Contatos.Where(x => x.IdPessoaJuridica == id).FirstOrDefault();
        }
    }
}