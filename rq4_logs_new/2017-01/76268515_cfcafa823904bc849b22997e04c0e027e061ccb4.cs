using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Android.App;
using Android.Content;
using Android.OS;
using Android.Runtime;
using Android.Views;
using Android.Widget;
using System.IO;
using Android.Graphics;
using System.Net;
using System.Collections.Specialized;



namespace SafeDeal.Android
{
    [Activity(Label = "CreateUserActivity")]
    public class CreateUserActivity : Activity
    {
        private ListView mListView;
        private BaseAdapter<User> mAdapter;
        private List<User> mUsers;
        private ImageView mSelectedPic;

        protected override void OnCreate(Bundle bundle)
        {

            base.OnCreate(bundle);
            SetContentView(Resource.Layout.CreateUserMain);

            mListView = FindViewById<ListView>(Resource.Id.createUserlistView);
            mUsers = new List<User>();

            Action<ImageView> action = PicSelected;

            mAdapter = new UserListAdapter(this, Resource.Layout.row_user, mUsers, action);
            mListView.Adapter = mAdapter;


        }

        private void PicSelected(ImageView selectedPic)
        {
            mSelectedPic = selectedPic;
            Intent intent = new Intent();
            intent.SetType("image/*");
            intent.SetAction(Intent.ActionGetContent);
            this.StartActivityForResult(Intent.CreateChooser(intent, "Selecte a Photo"), 0);
        }

        protected override void OnActivityResult(int requestCode, Result resultCode, Intent data)
        {
            base.OnActivityResult(requestCode, resultCode, data);

            if (resultCode == Result.Ok)
            {
                Stream stream = ContentResolver.OpenInputStream(data.Data);
                mSelectedPic.SetImageBitmap(BitmapFactory.DecodeStream(stream));
            }
        }

        //public override bool OnCreateOptionsMenu(IMenu menu)
        //{
        //    MenuInflater.Inflate(Resource.Menu.actionbar, menu);
        //    return base.OnCreateOptionsMenu(menu);
        //}

        public override bool OnOptionsItemSelected(IMenuItem item)
        {
            switch (item.ItemId)
            {
                case Resource.Id.add:

                    CreateUserDialog dialog = new CreateUserDialog();
                    FragmentTransaction transaction = FragmentManager.BeginTransaction();

                    //Subscribe to event
                    dialog.OnCreateUser += dialog_OnCreateUser;
                    dialog.Show(transaction, "create user");
                    return true;

                default:
                    return base.OnOptionsItemSelected(item);
            }

        }

        void dialog_OnCreateUser(object sender, CreateUserEventArgs e)
        {
            mUsers.Add(new User() { Name = e.Name, Number = e.Number });
            mAdapter.NotifyDataSetChanged();
        }
    }
}