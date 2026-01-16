using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MeetLife.Data.UnitToWork
{
    public class UnitToWorkDbContext : IUnitToWorkDbContext
    {
        private readonly IMeetLifeDbContext _dbContext;

        public UnitToWorkDbContext(IMeetLifeDbContext dbContext)
        {
            this._dbContext = dbContext;
        }

        public void Commit()
        {
            this._dbContext.SaveChanges();
        }
    }
}