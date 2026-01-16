package org.example.backend.controllers.admin.banHang;

import org.example.backend.constants.api.Admin;
import org.example.backend.dto.request.banHang.ThanhToanRequest;
import org.example.backend.models.ThanhToan;
import org.example.backend.repositories.ThanhToanRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ThanhToanController {
    @Autowired
    ThanhToanRepository thanhToanRepository;
    @GetMapping(Admin.PAY_GET_ALL)
    public ResponseEntity<?> getAll() {
        return ResponseEntity.ok(thanhToanRepository.getAllThanhToan());
    }

    @PostMapping(Admin.PAY_CREATE)
    public ResponseEntity<?> create(@RequestBody ThanhToanRequest thanhToanRequest) {
        ThanhToan t = new ThanhToan();
        t.setIdHoaDon(thanhToanRequest.getIdHoaDon());
        t.setPhuongThuc(thanhToanRequest.getPhuongThuc());
        t.setTienMat(thanhToanRequest.getTienMat());
        t.setTienChuyenKhoan(thanhToanRequest.getTienChuyenKhoan());
        t.setTongTien(thanhToanRequest.getTongTien());
        t.setNgayTao(thanhToanRequest.getNgayTao());
        t.setNgaySua(thanhToanRequest.getNgaySua());
        t.setTrangThai(thanhToanRequest.getTrangThai());
        t.setPhuongThucVnp(thanhToanRequest.getPhuongThucVnp());
        t.setMoTa(thanhToanRequest.getMoTa());
        t.setNguoiTao(thanhToanRequest.getNguoiTao());
        t.setNguoiSua(thanhToanRequest.getNguoiSua());
        return ResponseEntity.ok(thanhToanRepository.save(t));
    }
}