package org.example.backend.services;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.example.backend.common.PageResponse;
import org.example.backend.dto.response.dotGiamGia.DotGiamGiaResponse;
import org.example.backend.dto.response.phieuGiamGia.phieuGiamGiaReponse;
import org.example.backend.models.PhieuGiamGia;
import org.example.backend.repositories.PhieuGiamGiaRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
@Service
public class PhieuGiamGiaService extends GenericServiceImpl<PhieuGiamGia , UUID> {
    public PhieuGiamGiaService(JpaRepository<PhieuGiamGia, UUID> repository, PhieuGiamGiaRepository PGGRepository) {
        super(repository);
        this. PGGrepository= PGGRepository;
    }

//    public PageResponse<List<phieuGiamGiaReponse>> getAllPGG(int page, int itemsPerPage) {
//        Pageable pageable = PageRequest.of(page, itemsPerPage);
//        Page<phieuGiamGiaReponse> PGGPage = PGGrepository.getAllPhieuGiamGia(pageable);
//
//        return PageResponse.<List<phieuGiamGiaReponse>>builder()
//                .page(PGGPage.getNumber())
//                .size(PGGPage.getSize())
//                .totalPage(PGGPage.getTotalPages())
//                .items(PGGPage.getContent())
//                .build();
//    }

    private final PhieuGiamGiaRepository PGGrepository;

    public List<phieuGiamGiaReponse> getPGGGetAll() {
        return PGGrepository.getAllPhieuGiamGia();
    }

    public void setDeletedPhieuGiamGia(Boolean deleted, UUID id){
        PGGrepository.updateDetailPhieuGiamGia(deleted, id);
    }

    public PageResponse<List<phieuGiamGiaReponse>> searchPGG(int page, int itemsPerPage,String keyFind,
      String trangThai, Integer sapXep, Instant minNgay, Instant maxNgay, BigDecimal minGia, BigDecimal maxGia) {
        // Tạo đối tượng Sort dựa trên giá trị của tham số sapXep
        Sort sort = Sort.unsorted();  // Mặc định không sắp xếp nếu sapXep không khớp

        switch (sapXep) {
            case 1:
                sort = Sort.by(Sort.Order.desc("ngayTao")); // sapXep = 1, sắp xếp theo ngayTao giảm dần
                break;
            case 2:
                sort = Sort.by(Sort.Order.asc("ngayTao"));  // sapXep = 2, sắp xếp theo ngayTao tăng dần
                break;
            case 3:
                sort = Sort.by(Sort.Order.desc("ngaySua")); // sapXep = 3, sắp xếp theo ngaySua giảm dần
                break;
            default:
                break; // Không sắp xếp nếu sapXep không có giá trị mong đợi
        }

        // Tạo Pageable với phân trang và sắp xếp
        Pageable pageable = PageRequest.of(page, itemsPerPage, sort);


//        Pageable pageable = PageRequest.of(page, itemsPerPage);
        Page<phieuGiamGiaReponse> PGGPage = PGGrepository.searchPhieuGiamGia(pageable,keyFind,trangThai,minNgay,maxNgay,minGia,maxGia);
        return PageResponse.<List<phieuGiamGiaReponse>>builder()
                .page(PGGPage.getNumber())
                .size(PGGPage.getSize())
                .totalPage(PGGPage.getTotalPages())
                .items(PGGPage.getContent())
                .build();
    }

    public byte[] exportToExcel(List<phieuGiamGiaReponse> voucherList) throws IOException {
        Workbook workbook = new XSSFWorkbook();
        Sheet sheet = workbook.createSheet("Phiếu Giảm Giá");

        // Tạo header
        Row headerRow = sheet.createRow(0);
        headerRow.createCell(0).setCellValue("ID");
        headerRow.createCell(1).setCellValue("Mã");
        headerRow.createCell(2).setCellValue("Tên");
        headerRow.createCell(3).setCellValue("Loại");
        headerRow.createCell(4).setCellValue("Giá Trị");
        headerRow.createCell(5).setCellValue("Giảm Tối Đa");
        headerRow.createCell(6).setCellValue("Mức Độ");
        headerRow.createCell(7).setCellValue("Ngày Bắt Đầu");
        headerRow.createCell(8).setCellValue("Ngày Kết Thúc");
        headerRow.createCell(9).setCellValue("Số Lượng");
        headerRow.createCell(10).setCellValue("Điều Kiện");
        headerRow.createCell(11).setCellValue("Trạng Thái");

        int rowNum = 1;
        for (phieuGiamGiaReponse voucher : voucherList) {
            Row row = sheet.createRow(rowNum++);
            row.createCell(0).setCellValue(voucher.getId().toString());
            row.createCell(1).setCellValue(voucher.getMa());
            row.createCell(2).setCellValue(voucher.getTen());

            // Kiểm tra giá trị loai trước khi sử dụng
            Boolean loai = voucher.getLoai();
            if (loai != null) {
                row.createCell(3).setCellValue(loai ? "Phần trăm" : "Giá");
            } else {
                row.createCell(3).setCellValue("Chưa xác định"); // Hoặc giá trị mặc định nào đó
            }

            row.createCell(4).setCellValue(voucher.getGiaTri() != null ? voucher.getGiaTri().doubleValue() : 0.0);
            row.createCell(5).setCellValue(voucher.getGiamToiDa() != null ? voucher.getGiamToiDa().doubleValue() : 0.0);
            row.createCell(6).setCellValue(voucher.getMucDo() != null ? voucher.getMucDo() : "Không xác định");
            row.createCell(7).setCellValue(voucher.getNgayBatDau() != null ? voucher.getNgayBatDau().toString() : "Không xác định");
            row.createCell(8).setCellValue(voucher.getNgayKetThuc() != null ? voucher.getNgayKetThuc().toString() : "Không xác định");
            row.createCell(9).setCellValue(voucher.getSoLuong() != null ? voucher.getSoLuong() : 0);
            row.createCell(10).setCellValue(voucher.getDieuKien() != null ? voucher.getDieuKien() : "Không xác định");
            row.createCell(11).setCellValue(voucher.getTrangThai() != null ? voucher.getTrangThai() : "Không xác định");
        }

        // Ghi dữ liệu ra byte array
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        workbook.write(outputStream);
        workbook.close();

        return outputStream.toByteArray();
    }
}