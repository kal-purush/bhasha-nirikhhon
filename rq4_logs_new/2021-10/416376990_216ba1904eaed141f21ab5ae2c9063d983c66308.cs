using PopLibrary.SqlModels;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PopLibrary.Helpers
{
    public class FinalizeHelper
    {
        private readonly SqlAdapter _sqlAdapter;
        public FinalizeHelper(SqlAdapter sqlAdapter)
        {
            _sqlAdapter = sqlAdapter;
        }

        public void IngestAuctionResultsToPaymentTable(int auctionTypeId)
        {
            var auctionResults = _sqlAdapter.ExecuteStoredProcedureAsync<GetAuctionBidResult>("dbo.GetHighestBidForAllAuctionsOfType", new List<StoredProcedureParameter>
            {
                new StoredProcedureParameter { Name="@AuctionTypeId", DbType=SqlDbType.Int, Value=auctionTypeId }
            });
            foreach (var auctionResult in auctionResults)
            {
                // Check customers table, if no customer, create one
                var customerId = _sqlAdapter.ExecuteStoredProcedureAsync<int>("dbo.AddOrUpdateCustomer", new List<StoredProcedureParameter>
                {
                    new StoredProcedureParameter { Name="@Email", DbType=SqlDbType.NVarChar, Value=auctionResult.Email }
                }).FirstOrDefault();
                _sqlAdapter.ExecuteStoredProcedureAsync("dbo.AddOrUpdatePayment", new List<StoredProcedureParameter>
                {
                    new StoredProcedureParameter { Name="@AuctionId", DbType=SqlDbType.Int, Value=auctionResult.AuctionId },
                    new StoredProcedureParameter { Name="@CustomerId", DbType=SqlDbType.Int, Value=customerId },
                    new StoredProcedureParameter { Name="@Complete", DbType=SqlDbType.Bit, Value=0 }
                });
            }
        }
    }
}