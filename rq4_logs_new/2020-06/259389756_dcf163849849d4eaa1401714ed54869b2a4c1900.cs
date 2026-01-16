using AutoMapper;
using Microsoft.AspNetCore.Components;
using System.Collections.Generic;

namespace Hippologamus.Server.Pages
{
    public class PageComponentBase : ComponentBase
    {
        protected int _pageNumber;

        protected string _orderBy;

        public bool ShowDataAsAList { get; set; }

        public Dictionary<string, string> OrderByList { get; set; }

        public int ShowPageSize { get; set; }

        public bool OrderAscending { get; set; } = true;

        public void ShowDataAs_Click()
        {
            ShowDataAsAList = !ShowDataAsAList;
        }

        [Inject]
        public IMapper Mapper { get; set; }

        public string OrderBy
        {
            get
            {
                return _orderBy;
            }
            set
            {
                if (_orderBy == value)
                {
                    OrderAscending = !OrderAscending;
                }
                _orderBy = value;
            }
        }
    }
}