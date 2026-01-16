using Android.Support.V7.Widget;
using Android.Views;
using Exercise_6.Models;

namespace Exercise_6.Adapter
{
    public class CurrencyAdapter : RecyclerView.Adapter
    {
        private readonly CurrencyList currencyList;

        public CurrencyAdapter(CurrencyList currencyList)
        {
            this.currencyList = currencyList;
        }

        public override void OnBindViewHolder(RecyclerView.ViewHolder holder, int position)
        {
            if (holder is CurrencyViewHolder currencyViewHolder) currencyViewHolder.Currency = currencyList.Currencies[position];
        }

        public override RecyclerView.ViewHolder OnCreateViewHolder(ViewGroup parent, int viewType)
        {
            var itemView = LayoutInflater.From(parent.Context).Inflate(Resource.Layout.currency_item, parent, false);
            return new CurrencyViewHolder(itemView);
        }

        public override int ItemCount => currencyList.Currencies.Count;
    }
}