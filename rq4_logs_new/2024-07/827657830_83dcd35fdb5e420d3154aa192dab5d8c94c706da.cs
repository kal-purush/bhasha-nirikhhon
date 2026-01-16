using System;
using System.Collections.Generic;

namespace DTO.Models;

public partial class Hoadon
{
    public int IdHoaDon { get; set; }

    public int IdKh { get; set; }

    public int IdBan { get; set; }

    public DateOnly? NgayHoaDon { get; set; }

    public string? TenMonAn { get; set; }

    public string? CodeVoucher { get; set; }

    public decimal? TienGiam { get; set; }

    public decimal? TongTien { get; set; }

    public string? TrangThai { get; set; }

    public virtual Voucher? CodeVoucherNavigation { get; set; }

    public virtual ICollection<Hoadonchitiet> Hoadonchitiets { get; set; } = new List<Hoadonchitiet>();

    public virtual Ban IdBanNavigation { get; set; } = null!;

    public virtual Khachhang IdKhNavigation { get; set; } = null!;
}