using ProjectManagementSystem.API.Repositories;
using ProjectManagementSystem.DataAccessLayer.Context;
using ProjectManagementSystem.Model;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProjectManagementSystem.DataAccessLayer.Repository
{
    public class PaticipationHistoryRepository : GenericRepository<PaticipationHistory>, IPaticipationHistoryRepository
    {
        private GeneralContext context;
        private DbSet<PaticipationHistory> paticipationHistories;

        public PaticipationHistoryRepository(GeneralContext context)
            : base(context)
        {
            this.context = context;
            paticipationHistories = context.Set<PaticipationHistory>();
        }
    }
}