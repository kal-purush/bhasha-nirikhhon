using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ShopSim.Model.Abstract
{
    public abstract class Auditable :IAuditable
    {
        public DateTime? CreateDate { set; get; }

        [MaxLength(255)]
        public string CreateBy { set; get; }

        public DateTime? UploadDate { set; get; }

        [MaxLength(255)]
        public string UpdateBy { set; get; }
    }
}