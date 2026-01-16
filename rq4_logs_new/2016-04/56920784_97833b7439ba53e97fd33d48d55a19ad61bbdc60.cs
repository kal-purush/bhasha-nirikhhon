using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Android.App;
using Android.Content;
using Android.Graphics.Drawables;
using Android.OS;
using Android.Runtime;
using Android.Util;
using Android.Views;
using Android.Widget;

namespace OsmTest.Android.Adapter
{
   public class ListItem
   {
      public string Title { get; set; }
      public int ImageResourceId { get; set; }
   }
   public class ArrayAdapterWithIcon : ArrayAdapter<ListItem>
   {
      public ArrayAdapterWithIcon(IntPtr handle, JniHandleOwnership transfer) : base(handle, transfer)
      {
      }

      public ArrayAdapterWithIcon(Context context, int textViewResourceId) : base(context, textViewResourceId)
      {
      }

      public ArrayAdapterWithIcon(Context context, int resource, int textViewResourceId) : base(context, resource, textViewResourceId)
      {
      }

      public ArrayAdapterWithIcon(Context context, int textViewResourceId, ListItem[] objects) : base(context, textViewResourceId, objects)
      {
      }

      public ArrayAdapterWithIcon(Context context, int resource, int textViewResourceId, ListItem[] objects) : base(context, resource, textViewResourceId, objects)
      {
      }

      public ArrayAdapterWithIcon(Context context, int textViewResourceId, IList<ListItem> objects) : base(context, textViewResourceId, objects)
      {
      }

      public ArrayAdapterWithIcon(Context context, int resource, int textViewResourceId, IList<ListItem> objects) : base(context, resource, textViewResourceId, objects)
      {
      }

      public override View GetView(int position, View convertView, ViewGroup parent)
      {
         //var view = base.GetView(position, convertView, parent);

         //TextView textView = (TextView)view.FindViewById(Resource.Id.text1);
         ImageView imageView = new ImageView(Context) ;
         imageView.SetPadding(30,20,80,20);

         ListItem item = this.GetItem(position);
         imageView.SetImageResource(item.ImageResourceId);
         //if (Build.VERSION.SdkInt >= BuildVersionCodes.JellyBeanMr1)
         //{
         //   textView.SetCompoundDrawablesRelativeWithIntrinsicBounds(, 0, 0, 0);
         //}
         //else {
         //   textView.SetCompoundDrawablesRelativeWithIntrinsicBounds(images.get(position), 0, 0, 0);
         //}
         //textView.CompoundDrawablePadding = (int)TypedValue.ApplyDimension(ComplexUnitType.Dip, 12, Context.Resources.DisplayMetrics);

         return imageView;
      }
   }
}