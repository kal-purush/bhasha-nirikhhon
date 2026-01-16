using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Workflow_Models;
using Workflow_Models.Models;

namespace Workflow_BL.DAL
{
    public class KeywordRepository:GenericRepository<Keyword>
    {
        public KeywordRepository(DatabaseConfiguration context) : base(context)
        {
            Entity = ((DatabaseConfiguration)context).Keyword;
        }

        public IEnumerable<Keyword> GetAllKeywords()
        {
            return Entity.ToList();
        }
    }
}