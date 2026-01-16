package com.github.classpick.room.controller;

import com.github.classpick.global.dto.Response;
import com.github.classpick.room.service.RoomExcelService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@Tag(name = "강의실 엑셀 업로드")
@RestController
@RequiredArgsConstructor
public class RoomExcelController {

    private final RoomExcelService roomExcelService;

    @Operation(summary = "엑셀 업로드를 통한 강의실 생성")
    @PostMapping(value = "/v0.0/rooms/excel/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Response<Void> uploadRomExcel(@RequestParam MultipartFile file) {

        roomExcelService.uploadRoomExcel(file);
        return Response.ok();
    }
}