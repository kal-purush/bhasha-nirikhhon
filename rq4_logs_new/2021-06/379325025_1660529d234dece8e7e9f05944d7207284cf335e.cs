using BELBModels;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BELBDL;

namespace BELBBL
{
    public class LeaderboardBL : ILeaderboardBL
    {
        private readonly Repo _repo;
        public LeaderboardBL(BELBDBContext context)
        {
            _repo = new Repo(context);
        }
        public async Task<List<LeaderBoard>> GetAllCategories()
        {
            return await _repo.GetAllLeaderboards();
        }
        public async Task<LeaderBoard> GetLeaderboardById(int id)
        {
            return await _repo.GetLeaderboardById(id);
        }
    }
}