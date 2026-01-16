using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GW2Trader.Model;
using GW2TPApiWrapper;
using GW2TPApiWrapper.Wrapper;

namespace GW2Trader.Data
{
    public interface IItemRepository
    {
        GameItemModel ItemById(int id);
        IEnumerable<GameItemModel> ItemsById(int[] ids);
        IEnumerable<GameItemModel> Items();

        void Add(GameItemModel item);
        void Add(IEnumerable<GameItemModel> items);

        void Update(GameItemModel item);

        void RebuildItemDictionary();
        void RebuildItemList();
    }
}