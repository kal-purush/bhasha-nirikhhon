using System;
using System.Collections.Generic;
using Android.Support.V7.Widget;
using Android.Views;

namespace NotesApp.Droid
{
    class RecyclerViewAdapter : RecyclerView.Adapter
    {
        public List<String> dataset;
        public RecyclerViewAdapter(List<String> dataset)
        {
            this.dataset = dataset;
        }

        public override int ItemCount
        {
            get { return this.dataset.Count;  }
        }

        public override void OnBindViewHolder(RecyclerView.ViewHolder holder, int position)
        {
            RecyclerViewHolder vh = holder as RecyclerViewHolder;
            vh.text.Text = dataset[position];
        }

        public override RecyclerView.ViewHolder OnCreateViewHolder(ViewGroup parent, int viewType)
        {
            View itemView = LayoutInflater.From(parent.Context).Inflate(Resource.Layout.RecyclerCardView, parent, false);
            RecyclerViewHolder vh = new RecyclerViewHolder(itemView);
            return vh;
        }
    }
}