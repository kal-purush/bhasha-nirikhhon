using System.ComponentModel.DataAnnotations;

namespace Moridge.BusinessLogic
{
    public static class Occasions
    {
        public enum Occassions
        {
            [Display(Name = "Förmiddag")]
            Morning,
            [Display(Name = "Eftermiddag")]
            Afternoon
        }
    }
}