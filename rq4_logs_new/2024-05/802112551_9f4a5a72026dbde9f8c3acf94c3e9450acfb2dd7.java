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
@Table(name = "GiayTheThaoChiTiet")
public class GiayTheThaoChiTiet {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "Id_GiayTheThaoChiTiet")
    private UUID id;

    @ManyToOne
    @JoinColumn(name = "Id_GiayTheThao")
    private GiayTheThao giayTheThao;

    @ManyToOne
    @JoinColumn(name = "Id_Size")
    private Size size;

    @ManyToOne
    @JoinColumn(name = "Id_MauSac")
    private MauSac mauSac;

    @Column(name = "soLuong")
    private String soLuong;

    @Column(name = "ghiChu")
    private String ghiChu;

    @Column(name = "ngayTao")
    private String ngayTao;

    @Column(name = "ngaySua")
    private String ngaySua;

    @Column(name = "trangThai")
    private int trangThai;
}