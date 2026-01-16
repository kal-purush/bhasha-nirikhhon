using Microsoft.AspNetCore.Mvc.Rendering;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace WhoWantsWhat.Models.ViewModels.GiftListsViewModels
{
    public class EditGiftListViewModel
    {
        public GiftList GiftList { get; set; }
        public List<ApplicationUser> Receivers { get; set; } = new List<ApplicationUser>();
        public List<SelectListItem> ReceiverOptions
        {
            get
            {
                if (Receivers == null) return null;

                List<SelectListItem> selectItems = Receivers
                    .Select(r => new SelectListItem(r.FirstName, r.Id.ToString()))
                    .ToList();
                selectItems.Insert(0, new SelectListItem
                {
                    Text = "Who is this list for?",
                    Value = ""
                });
                selectItems.Add(new SelectListItem
                {
                    Text = "Other",
                    Value = "0"
                });

                return selectItems;
            }
        }
        public List<ListType> ListTypes { get; set; } = new List<ListType>();
        public List<SelectListItem> ListTypeOptions
        {
            get
            {
                if (ListTypes == null) return null;

                List<SelectListItem> selectItems = ListTypes
                    .Select(lt => new SelectListItem(lt.Label, lt.ListTypeId.ToString()))
                    .ToList();
                selectItems.Insert(0, new SelectListItem
                {
                    Text = "Choose list type...",
                    Value = ""
                });

                return selectItems;
            }
        }
    }
}