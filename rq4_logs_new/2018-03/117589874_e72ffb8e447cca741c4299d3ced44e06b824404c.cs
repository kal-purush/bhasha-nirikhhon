using System;
using Lykke.Job.PayTransactionHandler.Core.Domain.TransactionStateCache;

namespace Lykke.Job.PayTransactionHandler.Core.Extensions
{
    public static class TransactionStateExtensions
    {
        public static bool IsConfirmed(this TransactionState src, int confirmationsLimit)
        {
            return src.Transaction.Confirmations >= confirmationsLimit;
        }

        public static bool IsExpired(this TransactionState src)
        {
            return src.DueDate < DateTime.UtcNow;
        }
    }
}