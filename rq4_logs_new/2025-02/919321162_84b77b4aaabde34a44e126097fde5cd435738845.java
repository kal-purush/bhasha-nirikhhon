package com.fptgang.backend.controller;

import com.fptgang.backend.api.controller.SkusApi;
import com.fptgang.backend.api.model.*;
import com.fptgang.backend.mapper.StockKeepingUnitMapper;
import com.fptgang.backend.model.Account;
import com.fptgang.backend.service.StockKeepingUnitService;
import com.fptgang.backend.util.OpenApiHelper;
import com.fptgang.backend.util.SecurityUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/api/v1")
public class StockKeepingUnitController implements SkusApi {

    private final StockKeepingUnitService stockKeepingUnitService;

    private final StockKeepingUnitMapper stockKeepingUnitMapper;

    public StockKeepingUnitController(StockKeepingUnitService stockKeepingUnitService, StockKeepingUnitMapper stockKeepingUnitMapper) {
        this.stockKeepingUnitService = stockKeepingUnitService;
        this.stockKeepingUnitMapper = stockKeepingUnitMapper;
    }


    @Override
    public ResponseEntity<StockKeepingUnitDto> createStockKeepingUnit(StockKeepingUnitDto stockKeepingUnitDto) {
        if (!SecurityUtil.isRole(Account.Role.ADMIN, Account.Role.STAFF)) {
            throw new AccessDeniedException("Only staff and admins can create blind boxes.");
        }
        ResponseEntity<StockKeepingUnitDto> response = new ResponseEntity<>(stockKeepingUnitMapper
                .toDTO(stockKeepingUnitService.create(stockKeepingUnitMapper.toEntity(stockKeepingUnitDto))), HttpStatus.CREATED);
        return response;
    }

    @Override
    public ResponseEntity<Void> deleteStockKeepingUnit(Long stockKeepingUnitId) {
        if (!SecurityUtil.hasPermission(Account.Role.ADMIN)) {
            throw new AccessDeniedException("Only admins can delete blind boxes.");
        }
        stockKeepingUnitService.deleteById(stockKeepingUnitId);
        return  new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }

    @Override
    public ResponseEntity<StockKeepingUnitDto> getStockKeepingUnitById(Long stockKeepingUnitId) {
        ResponseEntity<StockKeepingUnitDto> response = new ResponseEntity<>(stockKeepingUnitMapper
                .toDTO(stockKeepingUnitService.findById(stockKeepingUnitId)), HttpStatus.OK);
        return response;
    }

    @Override
    public ResponseEntity<GetStockKeepingUnits200Response> getStockKeepingUnits(Pageable pageable, String filter, String search) {
        org.springframework.data.domain.Page<StockKeepingUnitDto> res = null;
        log.info("Getting stockKeepingUnits" + pageable + filter + search);
        var includeInvisible = false;
        var page = OpenApiHelper.toPageable(pageable);
        try {
            includeInvisible = SecurityUtil.hasPermission(Account.Role.ADMIN);
        } catch (Exception e) {
            log.error("Error getting stockKeepingUnits", e.getMessage());
        }

        res = stockKeepingUnitService
                .getAll(page, filter, search, includeInvisible)
                .map(stockKeepingUnitMapper::toDTO);

        log.info(res.toString());
        return OpenApiHelper.respondPage(res, GetStockKeepingUnits200Response.class);
    }

    @Override
    public ResponseEntity<StockKeepingUnitDto> updateStockKeepingUnit(Long stockKeepingUnitId, StockKeepingUnitDto stockKeepingUnitDto) {
        if (!SecurityUtil.hasPermission(Account.Role.ADMIN)) {
            throw new AccessDeniedException("Only staff and admins can update blind boxes.");
        }
        stockKeepingUnitDto.setSkuId(stockKeepingUnitId); // Override stockKeepingUnitId

        ResponseEntity<StockKeepingUnitDto> response = new ResponseEntity<>(stockKeepingUnitMapper
                .toDTO(stockKeepingUnitService.update(stockKeepingUnitMapper.toEntity(stockKeepingUnitDto))), HttpStatus.OK);
        return response;
    }
}