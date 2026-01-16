using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using ExpenseTracker.Repository.Entities;

namespace ExpenseTracker.Repository.Dapper
{
    public class ExpenseTrackerDapperRepository : IExpenseTrackerDapperRepository
    {
        internal IDbConnection Connection => new SqlConnection(ConfigurationManager.ConnectionStrings["ExpenseTrackerContext"].ConnectionString);

        public IEnumerable<ExpenseGroup> GetAllExpenseGroups()
        {
            IEnumerable<ExpenseGroup> result;

            using (IDbConnection connection = Connection)
            {
                connection.Open();
                result = connection.Query<ExpenseGroup>("SELECT * FROM dbo.ExpenseGroup");
            }

            return result;
        }
    }
}