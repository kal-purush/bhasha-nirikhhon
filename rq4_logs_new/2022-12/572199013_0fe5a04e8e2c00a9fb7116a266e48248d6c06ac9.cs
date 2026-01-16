using Android.App;
using Android.Content;
using Android.OS;
using Android.Runtime;
using Android.Views;
using Android.Widget;
using AndroidX.RecyclerView.Widget;
using BethanysPieShopMobile.Utility;
using PieShop.Core.Models;
using PieShop.Core.Repository;
using PieShopMobile.ViewHolders;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace PieShopMobile.Adapters
{
    public class PieAdapter : RecyclerView.Adapter
    {
        private List<Pie> _pies;
        public event EventHandler<int> ItemClick;
        public PieAdapter()
        {
            var pieRepo = new PieRepository();
            _pies = pieRepo.GetAllPies();
        }
        public override int ItemCount => _pies.Count();

        public override void OnBindViewHolder(RecyclerView.ViewHolder holder, int position)
        {
            if (holder is PieViewHolder pieViewHolder)
            {
                pieViewHolder.PieNameTextView.Text = _pies[position].Name;

                var imageBitmap = ImageHelper.GetImageBitmapFromUrl(_pies[position].ImageThumbnailUrl);
                pieViewHolder.PieImageView.SetImageBitmap(imageBitmap);
            }
        }

        public override RecyclerView.ViewHolder OnCreateViewHolder(ViewGroup parent, int viewType)
        {
            View itemView =
              LayoutInflater.From(parent.Context).Inflate(Resource.Layout.pie_viewholder, parent, false);

            PieViewHolder pieViewHolder = new PieViewHolder(itemView, OnClick);
            return pieViewHolder;
        }

        void OnClick(int position)
        {
            var pieId = _pies[position].PieId;
            ItemClick?.Invoke(this, pieId);
        }
    }
}