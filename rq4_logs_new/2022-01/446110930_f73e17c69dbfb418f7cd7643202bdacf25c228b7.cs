using System.ComponentModel.DataAnnotations;

namespace VehiclePassRegister.Models.Request
{
    public class VehicleUpdateDto
    {
        [Required(ErrorMessage = "DriverName is requiesd")]
        [StringLength(50, MinimumLength = 2)]
        public string DriverName { get; set; }
        public int DriverPhoneNo { get; set; }
    }
}