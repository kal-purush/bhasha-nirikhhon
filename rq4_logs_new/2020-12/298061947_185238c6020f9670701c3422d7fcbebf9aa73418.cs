using SmartSaver.EntityFrameworkCore.Models;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace SmartSaver.EntityFrameworkCore.Repositories
{
    public enum CreateTransactionResponse
    {
        Success,
        BadAmountError,
        GeneralError
    }

    public interface ITransactionRepo
    {
        /// <summary>
        /// Method that returns all account transactions from spending category.
        /// </summary>
        /// <param name="accId"></param>
        /// <returns>Spendings</returns>
        List<Transaction> GetThisMonthAccountSpendings(int accId);

        /// <summary>
        /// Method that returns balance history (how balance changed) for an account.
        /// </summary>
        /// <param name="accId"></param>
        /// <returns>List of balance changes</returns>
        List<Balance> GetBalanceHistory(int accId);

        /// <summary>
        /// All transactions an account made.
        /// </summary>
        /// <param name="accId">Account id</param>
        /// <returns>List of transactions</returns>
        List<Transaction> GetByAccountId(int accId);

        Task<CreateTransactionResponse> CreateTransactionForAccount(Transaction transaction, int accountId);
        IEnumerable<Transaction> GetByAccountForDateRange(int accId, DateTime startData, DateTime endDate);

        Transaction GetById(int transactionId);
    }
}