using System.ComponentModel.DataAnnotations;

namespace _4.CusView.ModelRequest
{
    public class DangKyRequestModel
    {

        [Required(ErrorMessage = "Vui lòng nhập tên khách hàng")]
        public string? Ten { get; set; }
        [Required(ErrorMessage = "Vui lòng nhập email")]
        [RegularExpression(@"^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@" + "[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$", ErrorMessage = "Vui lòng nhập đúng email")]
        public string? Email { get; set; }
        [Required(ErrorMessage = "Vui lòng nhập mật khẩu")]
        [StringLength(30, MinimumLength = 6, ErrorMessage = "Vui lòng nhập tối thiểu 6 ký tự, tối đa 30 ký tự")]
        public string? MatKhau { get; set; }
        [Required(ErrorMessage = "Vui lòng nhập địa chỉ")]
        public string? DiaChi { get; set; }
        public bool? GioiTinh { get; set; }
        [Required(ErrorMessage = "Vui lòng nhập số điện thoại")]
        [DataType(DataType.PhoneNumber)]
        public string? Sdt { get; set; }
        [Required(ErrorMessage = "Vui lòng nhập ngày sinh")]
        public DateTime? NgaySinh { get; set; }
    }
}