using BoreholeData.Entities;
using BoreholeData.Enums;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;

namespace BoreholeMVCUI.Models
{
    public class CableViewModel : BoreholeViewModel
    {
        public CableViewModel()
        {
                
        }

        public CableViewModel(CableBorehole borehole)
        {

            this.Id = borehole.ID;
            this.X = borehole.X;
            this.Y = borehole.Y;
            this.Depth = borehole.Depth;
            this.CableType = borehole.CableType;
            this.CableStrength = borehole.CableStrength;
        }

        [DisplayName("Cable Type")]
        [Required]
        public CableType CableType { get; set; }


        [DisplayName("Cable Strength")]
        [Required]
        public string CableStrength { get; set; }
    }
}