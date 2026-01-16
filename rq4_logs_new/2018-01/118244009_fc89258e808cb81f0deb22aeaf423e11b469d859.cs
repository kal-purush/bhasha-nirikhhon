using Android.Support.V7.Widget;
using Android.Views;
using Exercise_2.Models;

namespace Exercise_2.Adapters
{
    public class UserAdapter : RecyclerView.Adapter
    {
        private readonly ListUser listUser;

        public UserAdapter(ListUser listUser)
        {
            this.listUser = listUser;
        }

        public override void OnBindViewHolder(RecyclerView.ViewHolder holder, int position)
        {
            if (holder is UserViewHolder viewHolder) viewHolder.User = listUser.Users[position];
        }

        public override RecyclerView.ViewHolder OnCreateViewHolder(ViewGroup parent, int viewType)
        {
            var itemView = LayoutInflater.From(parent.Context).Inflate(Resource.Layout.user_item, parent, false);
            return new UserViewHolder(itemView);
        }

        public override int ItemCount => listUser.TotalCount;
    }
}