using System;
using System.Collections.Generic;
using System.Linq;
using SuperERP.DAL.Models;
using System.Data.Entity;


namespace SuperERP.DAL.Repositories
{
    public class OrdemServicoRepositorio : Repositorio<OrdemServico>
    {
        public ICollection<OrdemServico> listarTudao()
        {
            return dbContext.OrdensDeServico
                .Include(x => x.Servico)
                .Include(x => x.Status_Servico)
                .ToList(); 
        }

        public OrdemServico buscarPorID(int ordemID)
        {
            /* var ordem = dbContext.OrdemServico.FirstOrDefault(x=> x.ID == ordemID);
             return ordem;*/
            return dbContext.OrdensDeServico.FirstOrDefault(x => x.Id == ordemID);

        }

        public void salvarOrdemServico(OrdemServico ordem)
        {
            try
            {
                dbContext.OrdensDeServico.Add(ordem);
            }
            catch (Exception)
            {

                throw;
            }

            dbContext.SaveChanges();
        }

        public void deletarOrdemServico(OrdemServico ordem)
        {
            dbContext.OrdensDeServico.Remove(ordem);
            dbContext.SaveChanges();
        }
    }
}