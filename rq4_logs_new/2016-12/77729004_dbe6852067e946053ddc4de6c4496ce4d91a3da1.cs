using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace blogfoo.Models.Blogs
{
    public class Entry
    {

        public string Author { get; set; }
        public string Title { get; set; }
        public string SubTitle { get; set; }
        public string content { get; set; }
        public DateTime EntryDate { get; set; }
    }
}