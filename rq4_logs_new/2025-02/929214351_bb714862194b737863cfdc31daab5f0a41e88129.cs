using iPhoneBE.Data.Model;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace iPhoneBE.Data.Entities
{
    public class Attribute
    {
        [Key]
        public int AttributeID { get; set; }

        [Required]
        [MaxLength(255)]
        public string AttributeName { get; set; }

        [MaxLength(255)]
        public string DataType { get; set; }

        public int CategoryID { get; set; }

        public virtual Category Category { get; set; }
        public virtual ICollection<ProductItemAttribute> ProductItemAttributes { get; set; }
    }
}