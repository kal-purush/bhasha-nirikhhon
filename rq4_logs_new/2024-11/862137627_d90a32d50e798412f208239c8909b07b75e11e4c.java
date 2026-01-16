package org.example.backend.controllers.admin.thongKe;

import org.example.backend.constants.api.Admin;
import org.example.backend.dto.response.thongKe.ThongKeResponse;
import org.example.backend.services.HoaDonChiTietService;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class ThongKeController {
    private final HoaDonChiTietService hoaDonChiTietService;

    public ThongKeController(HoaDonChiTietService hoaDonChiTietService) {
        this.hoaDonChiTietService = hoaDonChiTietService;
    }


//    @GetMapping(Admin.THONG_KE)
//    public List<ThongKeResponse> getThongKe() {
//        return hoaDonChiTietService.getThongKeData();
//    }
}