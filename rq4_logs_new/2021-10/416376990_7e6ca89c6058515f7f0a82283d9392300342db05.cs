using PopLibrary.SqlModels;
using System.Collections.Generic;
using System.Data;

namespace PopLibrary.Data
{
    public class PaymentData
    {
        private SqlAdapter _sqlAdapter;

        public PaymentData(SqlAdapter sqlAdapter)
        {
            _sqlAdapter = sqlAdapter;
        }

        public IEnumerable<Payment> GetPayments()
        {
            return _sqlAdapter.ExecuteStoredProcedure<Payment>("dbo.GetPayments"); //todo tiara stored proc
        }

        public void AddOrUpdatePayment(Payment itemToUpdate)
        {
            var parameters = new List<StoredProcedureParameter>();

            if (itemToUpdate.Id != null)
            {
                parameters.Add(new StoredProcedureParameter { Name = "@PaymentId", DbType = SqlDbType.Int, Value = itemToUpdate.Id });
            }

            parameters.AddRange(new List<StoredProcedureParameter>{
                    new StoredProcedureParameter { Name = "@AuctionId", DbType = SqlDbType.Int, Value = itemToUpdate.AuctionId},
                    new StoredProcedureParameter { Name = "@CustomerId", DbType = SqlDbType.Int, Value = itemToUpdate.CustomerId },
                    new StoredProcedureParameter { Name = "@Complete", DbType = SqlDbType.Bit, Value = itemToUpdate.Complete },
                    new StoredProcedureParameter { Name = "@StripeInvoiceItemId", DbType = SqlDbType.NVarChar, Value = itemToUpdate.StripeInvoiceItemId }
                    new StoredProcedureParameter { Name = "@StripeInvoiceId", DbType = SqlDbType.NVarChar, Value = itemToUpdate.StripeInvoiceId},
                    new StoredProcedureParameter { Name = "@Created", DbType = SqlDbType.Timestamp, Value = itemToUpdate.Created },
                    new StoredProcedureParameter { Name = "@Amount", DbType = SqlDbType.Decimal, Value = itemToUpdate.Amount },
                    new StoredProcedureParameter { Name = "@Description", DbType = SqlDbType.Text, Value = itemToUpdate.Description }
                }
            );

            _sqlAdapter.ExecuteStoredProcedure<int>("dbo.AddOrUpdatePayment", parameters); //todo tiara stored proc
        }

        public void DeletePayment(int paymentId)
        {
            var parameters = new List<StoredProcedureParameter>() {
                new StoredProcedureParameter { Name = "@PaymentId", DbType = SqlDbType.Int, Value = paymentId }
            };

            _sqlAdapter.ExecuteStoredProcedure<int>("dbo.DeletePayment", parameters); //todo tiara stored proc
        }
    }
}