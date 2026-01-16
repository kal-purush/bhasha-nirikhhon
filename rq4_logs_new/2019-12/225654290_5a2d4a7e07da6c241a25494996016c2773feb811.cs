using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace WhoWantsWhat.Models.ViewModels.ItemsViewModels
{
    public class EditItemViewModel
    {
        public Item Item { get; set; }
        public int? WishListId { get; set; }
        public WishList WishList { get; set; }
        public int WishListItemId { get; set; }
        public WishListItem WishListItem { get; set; }
        public int? GiftListId { get; set; }
        public GiftList GiftList { get; set; }
        public int GiftListItemId { get; set; }
        public GiftListItem GiftListItem { get; set; }
    }
}