using System.ComponentModel.DataAnnotations;

namespace YIT.__Domain.Entities._Enums
{
    public enum EnStatusEFT
    {
        [Display(Name = "")]
        None = 0,
        [Display(Name = "Tertangguh")]
        Pending = 1,
        [Display(Name = "Berjaya")]
        Success = 2,
        [Display(Name = "Gagal")]
        Fail = 3,
    }
}