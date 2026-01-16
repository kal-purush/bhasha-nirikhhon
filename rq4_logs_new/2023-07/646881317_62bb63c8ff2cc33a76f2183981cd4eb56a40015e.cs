namespace _4.CusView.ModelRequest
{
    public class SanPhamChiTietRequest
    {
        public string? message { get; set; }
        public int error { get; set; }
        public List<ViewSanPhamChiTiet>? data { get; set; }

    }
    public class ViewSanPhamChiTiet
    {
        public Guid Id { get; set; }
        public decimal? GiaBan { get; set; }
        public string? TrangThai { get; set; }
        public string? TenMauSac { get; set; }
        public string? MaSPChiTiet { get; set; }
        public string? TenSPChiTiet { get; set; }
        public string? AnhDaiDien { get; set; }
    }
}