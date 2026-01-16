using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Android.App;
using Android.Content;
using Android.OS;
using Android.Runtime;
using Android.Views;
using Android.Widget;
using Java.Lang;
using Models;

namespace PriceCompEngnMobile
{
    class AnalyzerListAdapter : ArrayAdapter
    {
        private List<AnalyzerEntry> _entries;

        public AnalyzerListAdapter(Context context, int resource, List<AnalyzerEntry> entries) 
            : base(context, resource, entries)
        {
            _entries = entries;
        }

        public override View GetView(int position, View convertView, ViewGroup parent)
        {
            if (convertView == null)
            {
                convertView = LayoutInflater.From(Context).Inflate(Resource.Layout.analyzed_list_row, parent, false);
            }

            TextView itemName = convertView.FindViewById<TextView>(Resource.Id.analyzer_item_name);
            TextView price = convertView.FindViewById<TextView>(Resource.Id.analyzer_item_price);
            TextView payedPrice = convertView.FindViewById<TextView>(Resource.Id.analyzer_item_payedprice);

            itemName.Text = _entries.ElementAt(position).ItemName;
            price.Text = _entries.ElementAt(position).Price;
            payedPrice.Text = _entries.ElementAt(position).PayedPrice;

            return convertView;
        }

        public new AnalyzerEntry GetItem(int position)
        {
            return _entries.ElementAt(position);
        }

        public List<AnalyzerEntry> GetItems()
        {
            return _entries;
        }
    }
}