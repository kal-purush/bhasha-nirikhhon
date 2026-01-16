using DAL.Data;
using DAL.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DAL.Repositories.DysfunctionRepositories
{
    public class DysfunctionRepository : GenericRepository<Dysfunction>, IDysfunctionRepository
    {
        public DysfunctionRepository(OnlineTeamScanContext context) : base(context)
        { }
    }
}