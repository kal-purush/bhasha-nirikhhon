using Buxfer.Client;
using Microsoft.Extensions.Configuration;
using System.Linq;
using System.Threading.Tasks;

namespace BuxferToYNAB.Services
{
    public class TransactionService
    {
        private readonly IConfiguration _config;
        private readonly string _buxferUser, _buxferPassword;

        public TransactionService(IConfiguration config)
        {
            _config = config;
            _buxferUser = _config["BuxferUser"];
            _buxferPassword = _config["BuxferPwd"];
        }

        public void SyncTransactions()
        {
            var transactionsBuxfer = GetTransactionsFromBaxferAsync();

            var filter = transactionsBuxfer.Result.Entities.Where(x => x.Date == new System.DateTime(2022, 04, 08)).ToList();
        }

        public async Task<FilterResult<Transaction>> GetTransactionsFromBaxferAsync()
        {
            var client = new BuxferClient(_buxferUser, _buxferPassword);

            var lastTransactions = await client.GetTransactions();
            return lastTransactions;


        }

    }
}