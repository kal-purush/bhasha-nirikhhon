package org.example.backend.controllers.admin.quanLyDonHang;

import org.example.backend.common.PageResponse;
import org.example.backend.common.ResponseData;
import org.example.backend.dto.response.quanLyDonHang.hoaDonChiTietReponse;
import org.example.backend.repositories.HoaDonChiTietRepository;
import org.example.backend.repositories.HoaDonRepository;
import org.example.backend.services.HoaDonChiTietService;
import org.example.backend.services.HoaDonService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.example.backend.constants.api.Admin.BILL_DETAIL_GET_BY_ID;

@RestController
public class hoaDonChiTietController {

    final HoaDonChiTietRepository hoaDonChiTietRepository;
    private final HoaDonChiTietService hoaDonChiTietService;
    private final HoaDonRepository hoaDonRepository;

    public hoaDonChiTietController(HoaDonChiTietRepository hoaDonChiTietRepository,
                                   HoaDonChiTietService hoaDonChiTietService, HoaDonRepository hoaDonRepository) {
        this.hoaDonChiTietRepository = hoaDonChiTietRepository;
        this.hoaDonChiTietService = hoaDonChiTietService;
        this.hoaDonRepository = hoaDonRepository;
    }

    @GetMapping(BILL_DETAIL_GET_BY_ID)
    public ResponseEntity<ResponseData<PageResponse<List<hoaDonChiTietReponse>>>> getAllBillPage(
            @RequestParam(value = "itemsPerPage", defaultValue = "5") int itemsPerPage,
            @RequestParam(value = "page", defaultValue = "0") int page,
            @PathVariable UUID id
    ) {
        PageResponse<List<hoaDonChiTietReponse>> hdctPage = hoaDonChiTietService.getHoaDonChiTiet(page, itemsPerPage,id);
        ResponseData<PageResponse<List<hoaDonChiTietReponse>>> responseData = ResponseData.<PageResponse<List<hoaDonChiTietReponse>>>builder()
                .message("Get paginated users done")
                .status(HttpStatus.OK.value())
                .data(hdctPage)
                .build();
        return ResponseEntity.ok(responseData);
    }
}