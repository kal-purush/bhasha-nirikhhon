using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GW2Trader.Model;

namespace GW2Trader.Data
{
    public interface IGameDataContext : IDisposable
    {
        IDbSet<GameItemModel> GameItems { get; set; }
        IDbSet<InvestmentWatchlistModel> InvestmentWatchlists { get; set; }
        IDbSet<ItemIdWatchlistModel> ItemIdWatchlists { get; set; }

        void Save();
        void BulkInsert(IList<GameItemModel> items);
    }
}