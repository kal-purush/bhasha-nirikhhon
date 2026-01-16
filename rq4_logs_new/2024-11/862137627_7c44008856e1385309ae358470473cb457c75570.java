package org.example.backend.controllers.client;

import org.example.backend.common.PageResponse;
import org.example.backend.common.ResponseData;
import org.example.backend.constants.api.Admin;
import org.example.backend.dto.request.phieuGiamGia.phieuGiamGiaRequestAdd;
import org.example.backend.dto.response.banHang.banHangClientResponse;
import org.example.backend.models.GioHang;
import org.example.backend.models.GioHangChiTiet;
import org.example.backend.models.PhieuGiamGia;
import org.example.backend.services.GioHangChiTietService;
import org.example.backend.services.SanPhamChiTietService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.example.backend.constants.api.Admin.CART_CREATE;
import static org.example.backend.constants.api.Admin.CART_DELETE;
import static org.example.backend.constants.api.Admin.CART_UPDATE;
import static org.example.backend.constants.api.Admin.VOUCHER_CREATE;

@RestController
public class banHangClientController {

    @Autowired
    SanPhamChiTietService sanPhamChiTietService;
    @Autowired
    GioHangChiTietService gioHangChiTietService;

    @GetMapping(Admin.SELL_CLIENT_GET_ALL)
    public ResponseEntity<?> getbanHangClient(@RequestParam(value = "itemsPerPage", defaultValue = "5") int itemsPerPage,
                                              @RequestParam(value = "page", defaultValue = "0") int page
    ){
        PageResponse<List<banHangClientResponse>> bhPage = sanPhamChiTietService.getbanHangClient(page, itemsPerPage);
        ResponseData<PageResponse<List<banHangClientResponse>>> responseData = ResponseData.<PageResponse<List<banHangClientResponse>>>builder()
                .message("Get all banHangCient done")
                .status(HttpStatus.OK.value())
                .data(bhPage)
                .build();
        return ResponseEntity.ok(responseData);
    }

    @PostMapping(CART_CREATE)
    public ResponseEntity<?> createCart(@RequestBody GioHangChiTiet ghAdd) {
//        GioHangChiTiet gh = new GioHangChiTiet();
        return ResponseEntity.ok().body(gioHangChiTietService.save(ghAdd));
    }

    @PostMapping(CART_UPDATE)
    public ResponseEntity<?> updateCart(@PathVariable UUID id,@RequestBody GioHangChiTiet ghUpdate) {
        ghUpdate.setId(id);
        return ResponseEntity.ok().body(gioHangChiTietService.save(ghUpdate));
    }

    @DeleteMapping(CART_DELETE)
    public ResponseEntity<?> deleteCart(@RequestParam List<UUID> listId) {
        for (UUID id : listId) {
            gioHangChiTietService.deleteById(id);
        }
        return ResponseEntity.noContent().build();
    }

}