using Microsoft.AspNetCore.Mvc.Rendering;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace WhoWantsWhat.Models.ViewModels.ItemsViewModels
{
    public class AddExistingItemToWishListViewModel
    {
        public int WishListId { get; set; }
        public int ItemId { get; set; }
        public List<WishList> WishLists { get; set; } = new List<WishList>();
        public List<SelectListItem> WishListOptions
        {
            get
            {
                if (WishLists == null) return null;

                List<SelectListItem> selectItems = WishLists
                    .Select(wl => new SelectListItem(wl.Name, wl.WishListId.ToString()))
                    .ToList();
                selectItems.Insert(0, new SelectListItem
                {
                    Text = "Choose wish list...",
                    Value = ""
                });

                return selectItems;
            }
        }
    }
}