using api.layer.Entities;
using System;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace api.layer.DataAccessLayer
{
    public class GitActionsDAO: IGitActionsDAO
    {

        public async Task<bool> SavePullRequestDetails(PullRequestEntity pullRequestEntity)
        {
            using (SqlConnection sql = new SqlConnection(@"Data Source=MUM02L7746\SQLEXPRESS;Initial Catalog=TODO;Integrated Security=False;Persist Security Info=False;User ID=sa;Password=Password123"))
            {
                try
                {
                    using (SqlCommand cmd = new SqlCommand("SP_Code_Review", sql))
                    {
                        cmd.CommandType = System.Data.CommandType.StoredProcedure;
                        cmd.Parameters.Add(new SqlParameter("@PRId", pullRequestEntity.number));
                        cmd.Parameters.Add(new SqlParameter("@Action", pullRequestEntity.action));
                        cmd.Parameters.Add(new SqlParameter("@UserId", pullRequestEntity.userid));
                        cmd.Parameters.Add(new SqlParameter("@Bugs", 0));
                        cmd.Parameters.Add(new SqlParameter("@CodeSmells", 0));
                        cmd.Parameters.Add(new SqlParameter("@SecurityHotspots", 0));
                        cmd.Parameters.Add(new SqlParameter("@CoverageInfo", string.Empty));
                        cmd.Parameters.Add(new SqlParameter("@DuplicationInfo", string.Empty));
                        cmd.Parameters.Add(new SqlParameter("@PRCreatedTime", pullRequestEntity.created_at));
                        cmd.Parameters.Add(new SqlParameter("@PRUpdatedTime", pullRequestEntity.updated_at));
                        await sql.OpenAsync();
                        await cmd.ExecuteNonQueryAsync();
                        return true;
                    }
                }
                catch(Exception ex)
                {
                    throw ex;
                }
            }
        }



    }
}