using PopLibrary.SqlModels;
using System.Collections.Generic;
using System.Data;

namespace PopLibrary.Data
{
    public class BidData
    {
        private SqlAdapter _sqlAdapter;

        public BidData(SqlAdapter sqlAdapter)
        {
            _sqlAdapter = sqlAdapter;
        }

        public IEnumerable<GetAuctionBidResult> GetBids()
        {
            return _sqlAdapter.ExecuteStoredProcedure<GetAuctionBidResult>("dbo.GetBids");
        }

        public void AddOrUpdateBid(GetAuctionBidResult itemToUpdate)
        {
            var parameters = new List<StoredProcedureParameter>();
            if (itemToUpdate.Id != null)
            {
                parameters.Add(new StoredProcedureParameter { Name = "@Id", DbType = SqlDbType.Int, Value = itemToUpdate.Id });
            }

            parameters.AddRange(new List<StoredProcedureParameter>{
                    new StoredProcedureParameter { Name = "@AuctionId", DbType = SqlDbType.Int, Value = itemToUpdate.AuctionId},
                    new StoredProcedureParameter { Name = "@Amount", DbType = SqlDbType.Decimal, Value = itemToUpdate.Amount },
                    new StoredProcedureParameter { Name = "@Email", DbType = SqlDbType.NVarChar, Value = itemToUpdate.Email },
                    new StoredProcedureParameter { Name = "@Timestamp", DbType = SqlDbType.DateTime, Value = itemToUpdate.Timestamp }
                }
            );

            _sqlAdapter.ExecuteStoredProcedure<int>("dbo.AddOrUpdateAuctionBid", parameters);
        }

        public void DeleteAuctionBid(int auctionBidId)
        {
            var parameters = new List<StoredProcedureParameter>() {
                new StoredProcedureParameter { Name = "@AuctionBidId", DbType = SqlDbType.Int, Value = auctionBidId }
            };

            _sqlAdapter.ExecuteStoredProcedure<int>("dbo.DeleteAuctionBid", parameters);
        }
    }
}