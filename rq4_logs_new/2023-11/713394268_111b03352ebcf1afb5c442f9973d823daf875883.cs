using System.ComponentModel.DataAnnotations;


namespace Garage2._0.Models.ViewModels
{
    public class KvittoViewModel
    {
        
        [Required]
        [RegularExpression(@"[A-Za-z]{3}[0-9]{2}[A-Za-z0-9]{1}")]
        [Display(Name = "Registreringsnummer")]
        public string RegNr { get; set; }

        [Display(Name = "Ankomst tid")]
        public DateTime AnkomstTid { get; set; }

        [Display(Name = "Utchechning tid")]
        public DateTime UtchecksTid { get; set; }

        [Display(Name = "Total parkeringstid")]
        public TimeSpan ParkeringsTid { get; set; }
        public int Pris { get; set; }
        

    }
}