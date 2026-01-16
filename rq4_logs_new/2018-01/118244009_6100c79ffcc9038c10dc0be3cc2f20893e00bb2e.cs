using Android.Support.V7.Widget;
using Android.Views;
using Exercise_3.Models;

namespace Exercise_3.Adapters
{
    public class ImageAdapter : RecyclerView.Adapter
    {
        private readonly SearchResult searchResult;

        public ImageAdapter(SearchResult searchResult)
        {
            this.searchResult = searchResult;
        }

        public override void OnBindViewHolder(RecyclerView.ViewHolder holder, int position)
        {
            if (holder is ImageViewHolder viewHolder) viewHolder.Image = searchResult.Images[position];
        }

        public override RecyclerView.ViewHolder OnCreateViewHolder(ViewGroup parent, int viewType)
        {
            var itemView = LayoutInflater.From(parent.Context).Inflate(Resource.Layout.image_item, parent, false);
            return new ImageViewHolder(itemView);
        }

        public override int ItemCount => searchResult.ResultCount;
    }
}