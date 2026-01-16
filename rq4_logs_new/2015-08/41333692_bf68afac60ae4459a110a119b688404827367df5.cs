using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web;

namespace SerPro.API.Models
{
    public class ItemModel
    {
        public string Title { get; set; }
        public string Description { get; set; }
        public HttpPostedFileBase Attachment { get; set; }
    }
}