using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;
using Server.Core;
using Server.Models;

namespace Server.Repository
{
    /// <summary>
    /// Card repository for EF
    /// </summary>
    public class CardRepository : Repository<Card>, ICardRepository
    {
        public CardRepository(SQLContext context) : base(context)
        {
        }

        public IEnumerable<Card> GetWithTransactions(Expression<Func<Card, bool>> predicate)
        {
            return _collection.Where(predicate).Include(x => x.Transactions).ToList();
        }
    }
}