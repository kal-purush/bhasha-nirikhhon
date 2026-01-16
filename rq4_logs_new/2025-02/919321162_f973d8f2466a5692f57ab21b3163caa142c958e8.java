package com.fptgang.backend.controller;

import com.fptgang.backend.api.controller.SetsApi;
import com.fptgang.backend.api.model.GetSets200Response;
import com.fptgang.backend.api.model.Pageable;
import com.fptgang.backend.api.model.SetDto;
import com.fptgang.backend.mapper.SetMapper;
import com.fptgang.backend.model.Account;
import com.fptgang.backend.service.SetService;
import com.fptgang.backend.util.OpenApiHelper;
import com.fptgang.backend.util.SecurityUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;

import java.util.Optional;

@Slf4j
@RestController
@RequestMapping("/api/v1")
public class SetController implements SetsApi {
    private final SetService setService;
    private final SetMapper setMapper;

    @Autowired
    public SetController(SetMapper setMapper, SetService setService) {
        this.setService = setService;
        this.setMapper = setMapper;
    }

    @Override
    public Optional<NativeWebRequest> getRequest() {
        return SetsApi.super.getRequest();
    }

    @Override
    public ResponseEntity<SetDto> createSet(SetDto setDto) {
        log.info("Creating set");
        ResponseEntity<SetDto> response = new ResponseEntity<>(setMapper
                .toDTO(setService.create(setMapper.toEntity(setDto))), HttpStatus.CREATED);
        return response;
    }

    @Override
    public ResponseEntity<Void> deleteSet(Integer setId) {
        log.info("Deleting set " + setId);
        setService.deleteById(Long.valueOf(setId));
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @Override
    public ResponseEntity<SetDto> getSetById(Integer setId) {
        log.info("Getting set by id " + setId);
        return new ResponseEntity<>(setMapper.toDTO(setService.findById(Long.valueOf(setId))), HttpStatus.OK);
    }

    @Override
    public ResponseEntity<GetSets200Response> getSets(Pageable pageable, String filter, String search) {
        log.info("Getting sets");
        var page = OpenApiHelper.toPageable(pageable);
        var includeInvisible = SecurityUtil.hasPermission(Account.Role.ADMIN);
        var res = setService.getAll(page, filter, search, includeInvisible).map(setMapper::toDTO);
        return OpenApiHelper.respondPage(res, GetSets200Response.class);
    }

    @Override
    public ResponseEntity<SetDto> updateSet(Integer setId, SetDto setDto) {
        log.info("Updating set " + setId);
        return ResponseEntity.ok(setMapper.toDTO(setService.update(setMapper.toEntity(setDto))));
    }
}