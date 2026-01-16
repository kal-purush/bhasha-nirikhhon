package com.fptgang.backend.controller;

import com.fptgang.backend.api.controller.ShippingInfoApi;
import com.fptgang.backend.api.model.*;
import com.fptgang.backend.mapper.ShippingInfoMapper;
import com.fptgang.backend.model.Account;
import com.fptgang.backend.model.ShippingInfo;
import com.fptgang.backend.service.ShippingInfoService;
import com.fptgang.backend.util.OpenApiHelper;
import com.fptgang.backend.util.SecurityUtil;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1")
public class ShippingInfoController implements ShippingInfoApi {
    private final ShippingInfoService shippingInfoService;
    private final ShippingInfoMapper shippingInfoMapper;

    public ShippingInfoController(ShippingInfoService shippingInfoService, ShippingInfoMapper shippingInfoMapper) {
        this.shippingInfoService = shippingInfoService;
        this.shippingInfoMapper = shippingInfoMapper;
    }
    
    @Override
    public ResponseEntity<ShippingInfoDto> createShippingInfo(ShippingInfoDto shippingInfoDto) {
        return new ResponseEntity<>(shippingInfoMapper.toDTO(
                shippingInfoService.create(shippingInfoMapper.toEntity(shippingInfoDto))), HttpStatus.CREATED);
    }

    @Override
    public ResponseEntity<Void> deleteShippingInfo(Long shippingInfoId) {
        ShippingInfo shippingInfo = shippingInfoService.findById(shippingInfoId);
        if (!SecurityUtil.hasPermission(Account.Role.ADMIN) || SecurityUtil.getCurrentUserId()!=shippingInfo.getAccount().getAccountId()) {
            throw new AccessDeniedException("Only admins can delete shipping infos.");
        }
        shippingInfoService.deleteById(shippingInfoId);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @Override
    public ResponseEntity<ShippingInfoDto> getShippingInfoById(Long shippingInfoId) {
        
        return new ResponseEntity<>(shippingInfoMapper.toDTO(shippingInfoService.findById(shippingInfoId)), HttpStatus.OK);
    }

    @Override
    public ResponseEntity<GetShippingInfos200Response> getShippingInfos(Pageable pageable, String filter, String search) {
        var page = OpenApiHelper.toPageable(pageable);
        var includeInvisible = SecurityUtil.hasPermission(Account.Role.ADMIN);
        var res = shippingInfoService
                .getAll(page, filter, search, includeInvisible)
                .map(shippingInfoMapper::toDTO);
        return OpenApiHelper.respondPage(res, GetShippingInfos200Response.class);
    }

    @Override
    public ResponseEntity<ShippingInfoDto> updateShippingInfo(Long shippingInfoId, ShippingInfoDto shippingInfoDto) {
        ShippingInfo shippingInfo = shippingInfoService.findById(shippingInfoId);
        if (!SecurityUtil.hasPermission(Account.Role.ADMIN) || SecurityUtil.getCurrentUserId()!=shippingInfo.getAccount().getAccountId()) {
            throw new AccessDeniedException("Only owner and admins can update shipping infos.");
        }
        shippingInfoDto.setShippingInfoId(shippingInfoId); // Override shippingInfoId

        ResponseEntity<ShippingInfoDto> response = new ResponseEntity<>(shippingInfoMapper
                .toDTO(shippingInfoService.update(shippingInfoMapper.toEntity(shippingInfoDto))), HttpStatus.OK);
        return response;
    }
}