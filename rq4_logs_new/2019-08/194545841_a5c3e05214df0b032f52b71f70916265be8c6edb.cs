using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.EntityFrameworkCore;
using VillageDrill.Models.Objects;
using VillageDrill.Models.Repositories;

namespace VillageDrill.Areas.Admin.Pages
{
    public class AddAssemblyPartModel : PageModel
    {
        public struct SelectedPart
        {
            public bool isSelected;
            public int ItemId;
            public string ItemName;
            public int Qty;
        }

        private IRepositoryWrapper _repositoryWrapper;
        public List<Item> Items { get; set; }

        public SelectedPart AssemblyPart;


        public AddAssemblyPartModel(IRepositoryWrapper repositoryWrapper)
        {
            _repositoryWrapper = repositoryWrapper;
        }

        public async Task OnGet()
        {
            var items = _repositoryWrapper.Item.FindAll();
            Items = await items.ToListAsync();
        }
    }

}