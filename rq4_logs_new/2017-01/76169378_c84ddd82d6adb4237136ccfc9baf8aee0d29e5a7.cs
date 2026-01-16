using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Workflow_Models;
using Workflow_Models.Models;

namespace Workflow_BL.DAL
{
    public class FluxRepository:GenericRepository<Flux>
    {
        public FluxRepository(DatabaseConfiguration context) : base(context)
        {
            Entity = ((DatabaseConfiguration)context).Flux;
        }

        internal void AddFlux(Flux flux)
        {
            Create(flux);
            Context.SaveChanges();
        }
    }
}