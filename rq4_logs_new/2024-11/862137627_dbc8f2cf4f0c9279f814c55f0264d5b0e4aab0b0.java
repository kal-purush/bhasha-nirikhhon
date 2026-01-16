package org.example.backend.dto.request.traHang;

import jakarta.persistence.*;
import lombok.*;
import org.example.backend.models.SanPhamChiTiet;
import org.hibernate.annotations.ColumnDefault;
import org.hibernate.annotations.Nationalized;

import java.time.Instant;
import java.util.UUID;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class traHangRequest {

    private UUID id;

    private SanPhamChiTiet idSpct;

    private Integer soLuong;

    private String lyDo;

    private Instant ngayTao;

    private Instant ngaySua;

    private String nguoiTao;

    private String nguoiSua;

    private String trangThai;

    private Boolean deleted;
}