using System.ComponentModel.DataAnnotations;

namespace ZiekenFonds.API.Models
{
    public class Review
    {
        [Required(ErrorMessage = "Id is verplicht!")]
        public int Id { get; set; }
        [Required(ErrorMessage = "PersoonId is verplicht!")]

        public string PersoonId { get; set; }
        [Required(ErrorMessage = "BestemmingId is verplicht!")]

        public int BestemmingId { get; set; }

        public string Tekst {  get; set; }
        [Required(ErrorMessage = "Score is verplicht!")]

        public int Score { get; set; }
    }
}