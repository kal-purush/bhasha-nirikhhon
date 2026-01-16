package com.github.classpick.room.controller;

import com.github.classpick.global.dto.Response;
import com.github.classpick.room.controller.dto.request.RoomCreateRequest;
import com.github.classpick.room.controller.dto.response.RoomCreateResponse;
import com.github.classpick.room.service.RoomAdminService;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "어드민")
@RestController
@RequiredArgsConstructor
public class RoomAdminController {

    private final RoomAdminService roomAdminService;

    @PostMapping("v0.0/admin/rooms")
    public Response<RoomCreateResponse> createRoom(@RequestBody @Valid RoomCreateRequest request) {
        RoomCreateResponse response = roomAdminService.createRoom(request);
        return Response.ok(response);
    }

}