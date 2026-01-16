package com.example.sd_57_datn.Model;

import jakarta.persistence.*;
import lombok.*;

import java.util.UUID;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Builder
@Entity
@Table(name = "ChuongTrinhGiamGiaChiTietGiayTheThao")
public class ChuongTrinhGiamGiaChiTietGiayTheThao {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "Id_ChuongTrinhGiamGiaChiTietGiayTheThao")
    private UUID id;

    @ManyToOne
    @JoinColumn(name = "Id_ChuongTrinhGiamGiaChiTietGiayTheThao", referencedColumnName = "Id_ChuongTrinhGiamGiaGiayTheThao")
    private ChuongTrinhGiamGiaChiTietGiayTheThao chuongTrinhGiamGiaChiTietGiayTheThao;

    @ManyToOne
    @JoinColumn(name = "Id_GiayTheThao", referencedColumnName = "Id_GiayTheThao")
    private GiayTheThao giayTheThao;

    @Column(name = "soTienDaGiam")
    private String soTienDaGiam;

    @Column(name = "ghiChu")
    private String ghiChu;

    @Column(name = "ngayTao")
    private String ngayTao;

    @Column(name = "ngaySua")
    private String ngaySua;

    @Column(name = "TrangThai")
    private int trangThai;

}