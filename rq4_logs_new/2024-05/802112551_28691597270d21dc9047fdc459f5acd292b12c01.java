package com.example.sd_57_datn.Model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Date;
import java.util.UUID;

@Entity
@Table(name = "ChatLieu")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ChatLieu {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "Id_ChatLieu")
    private UUID id;

    @Column(name = "tenChatLieu")
    private String tenChatLieu;

    @Column(name = "ghiChu")
    private String ghiChu;

    @Column(name = "ngayTao")
    private Date ngayTao;

    @Column(name = "ngaySua")
    private Date ngaySua;

    @Column(name = "trangThai")
    private int trangThai;
}