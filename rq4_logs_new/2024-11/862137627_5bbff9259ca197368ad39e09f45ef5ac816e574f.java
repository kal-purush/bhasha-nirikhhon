package org.example.backend.dto.response.SanPham;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;
import java.util.UUID;
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class SanPhamClientResponse {
    private UUID id;
    private UUID idSanPham;
    private String ten;
    private UUID idHang;
    private String hang;
    private UUID idDanhMuc;
    private String danhMuc;
    private UUID idDeGiay;
    private String deGiay;
    private UUID idMauSac;
    private String mauSac;
    private UUID idKichThuoc;
    private String kichThuoc;
    public int soLuong;
    public BigDecimal giaBan;
    public BigDecimal giaNhap;
    public BigDecimal giaGiam;
    public String trangThai;

    public String hinhAnh;
    public String moTa;
}