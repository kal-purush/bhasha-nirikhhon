using Android.Support.V7.Widget;
using Android.Views;
using Exercise_5.Models;

namespace Exercise_5.Adapters
{
    public class CityAdapter : RecyclerView.Adapter
    {
        private readonly SjcGoldRate sjcGoldRate;

        public CityAdapter(SjcGoldRate sjcGoldRate)
        {
            this.sjcGoldRate = sjcGoldRate;
        }

        public override void OnBindViewHolder(RecyclerView.ViewHolder holder, int position)
        {
            if (holder is CityViewHolder viewHolder) viewHolder.City = sjcGoldRate.CityList.Cities[position];
        }

        public override RecyclerView.ViewHolder OnCreateViewHolder(ViewGroup parent, int viewType)
        {
            var itemView = LayoutInflater.From(parent.Context).Inflate(Resource.Layout.city_item, parent, false);
            return new CityViewHolder(itemView);
        }

        public override int ItemCount => sjcGoldRate.CityList.Cities.Count;
    }
}