using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ArvidsonFoto.Models
{
    public class TblPageCounter
    {
        public int Id { get; set; }

        /// <summary> Antal sidvisningar </summary>
        public int PageViews { get; set; }

        /// <summary> Sidans namn </summary>
        public string PageName { get; set; }

        /// <summary> Innehåller månaden som sidan visades, exempel: "2021-03" </summary>
        public string MonthViewed { get; set; }

        /// <summary> Datum när sidan senast visades </summary>
        public DateTime LastShowDate { get; set; }
    }
}