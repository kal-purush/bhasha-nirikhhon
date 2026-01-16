using MarisDoodleLibrary.Contracts.Db;
using MarisDoodleLibrary.Contracts.Repos;
using MarisDoodleLibrary.Db;
using MarisDoodleLibrary.Models;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace MarisDoodleLibrary.Repos
{
    public class SqlVoteRepo : IVoteRepo
    {
        private readonly IDataAccess _db;
        private readonly ConnectionStringData _connectionStringData;

        public SqlVoteRepo(IDataAccess db, ConnectionStringData connectionStringData)
        {
            _db = db;
            _connectionStringData = connectionStringData;
        }

        public async Task SaveVotes(List<VoteModel> votes)
        {
            string sql = "insert into dbo.Votes (VoterName, OptionId, CreatedOn) values (@VoterName, @OptionId, @CreatedOn);";

            foreach (var vote in votes)
            {
                await _db.Save(sql, new { VoterName = vote.VoterName, OptionId = vote.OptionId, CreatedOn = vote.CreatedOn }, _connectionStringData.SqlConnectionName);
            }
        }

    }
}