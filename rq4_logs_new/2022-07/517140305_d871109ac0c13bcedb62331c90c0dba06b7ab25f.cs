using System.Collections.Generic;
using System.Data;
using Dapper;
using IBM.Data.DB2.Core;

namespace CoreDockerApi
{
    /// <summary>
    /// Implements interface for get some dummy data.
    /// 
    /// You just need to change data that you are querying.
    /// </summary>
    public class DummyRepository : IDummyRepository
    {
        private readonly IDbConnection db;

        public DummyRepository(string strConnection)
        {
            db = new DB2Connection(strConnection);
        }
        
        public IList<ProductModel> GetDummyData()
        {
            return db.Query<ProductModel>(QueryDummyData).AsList();
        }

        private string QueryDummyData 
        {
            get 
            {
                return "SELECT * FROM TEST.PRODUCTS";
            }
        }
    }
}