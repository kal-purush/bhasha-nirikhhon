using System.Collections.Generic;
using Android.Support.V7.Widget;
using Android.Views;
using Exercise_1.Models;

namespace Exercise_1.Adapters
{
    public class TweetAdapter : RecyclerView.Adapter
    {
        private readonly List<Tweet> tweets;

        public TweetAdapter(List<Tweet> tweets)
        {
            this.tweets = tweets;
        }

        public override void OnBindViewHolder(RecyclerView.ViewHolder holder, int position)
        {
            if (holder is TweetViewHolder viewHolder) viewHolder.Tweet = tweets[position];
        }

        public override RecyclerView.ViewHolder OnCreateViewHolder(ViewGroup parent, int viewType)
        {
            var itemView = LayoutInflater.From(parent.Context).Inflate(Resource.Layout.tweet_item, parent, false);
            return new TweetViewHolder(itemView);
        }

        public override int ItemCount => tweets.Count;
    }
}