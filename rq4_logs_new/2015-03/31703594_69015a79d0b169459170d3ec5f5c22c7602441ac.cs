using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.ComponentModel.DataAnnotations;

namespace projectWebKassa.Models
{
    public class CategorieViewModels
    {
    }
    /// <summary>
    ///  hier worden categorieId aan ge maakt en aan een productId( wordt gekoppeld aan product naam) gekoppeld
    /// </summary>
    public class CategorieCreateViewModels {

        [Required]
        [Display(Name = "filiaal_codeId")]
        [MaxLength(1000)]
        [DataType(DataType.Text)]

        public string filiaal_codeId { get; set; }
    }
}