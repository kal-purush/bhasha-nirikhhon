package com.artiexh.api.controller;

import com.artiexh.api.base.common.Endpoint;
import com.artiexh.api.exception.ErrorCode;
import com.artiexh.api.service.ProvidedProductBaseService;
import com.artiexh.data.jpa.entity.ProvidedProductBaseId;
import com.artiexh.model.domain.ProvidedProductBase;
import com.artiexh.model.mapper.ProvidedProductBaseMapper;
import com.artiexh.model.rest.PageResponse;
import com.artiexh.model.rest.PaginationAndSortingRequest;
import com.artiexh.model.rest.providedproduct.ProvidedProductBaseDetail;
import com.artiexh.model.rest.provider.filter.ProviderProductFilter;
import jakarta.persistence.EntityNotFoundException;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springdoc.core.annotations.ParameterObject;
import org.springframework.data.domain.Page;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequiredArgsConstructor
@RequestMapping()
public class ProvidedProductController {
	private final ProvidedProductBaseService providedProductBaseService;
	private final ProvidedProductBaseMapper providedProductBaseMapper;
	@GetMapping(Endpoint.ProvidedProduct.ROOT)
	public PageResponse<ProvidedProductBaseDetail> getAll(
		@ParameterObject PaginationAndSortingRequest paginationAndSortingRequest,
		@ParameterObject ProviderProductFilter filter) {
		Page<ProvidedProductBase> productPage = providedProductBaseService.getAll(filter.getSpecification(), paginationAndSortingRequest.getPageable());
		return new PageResponse<>(productPage.map(providedProductBaseMapper::domainToDetail));
	}
	//Create Provided Product
	@PostMapping(Endpoint.Provider.ROOT + Endpoint.ProvidedProduct.DETAIL)
	@PreAuthorize("hasAuthority('ADMIN')")
	public ProvidedProductBaseDetail createProvidedProduct(
		@PathVariable("providerId") String businessCode,
		@PathVariable("productBaseId") Long productBaseId,
		@Valid @RequestBody ProvidedProductBaseDetail detail) {
		ProvidedProductBase providedProduct = providedProductBaseMapper.detailToDomain(detail);
		providedProduct.setProvidedProductBaseId(ProvidedProductBaseId.builder()
			.productBaseId(productBaseId)
			.businessCode(businessCode)
			.build());
		try {
			providedProduct = providedProductBaseService.create(providedProduct);
			return providedProductBaseMapper.domainToDetail(providedProduct);
		} catch (IllegalArgumentException exception) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
				exception.getMessage(),
				exception);
		}

	}


	//Update Provided Product
	@PutMapping(Endpoint.Provider.ROOT + Endpoint.ProvidedProduct.DETAIL)
	@PreAuthorize("hasAuthority('ADMIN')")
	public ProvidedProductBaseDetail updateProvidedProduct(
		@PathVariable("providerId") String businessCode,
		@PathVariable("productBaseId") Long productBaseId,
		@Valid @RequestBody ProvidedProductBaseDetail detail) {
		ProvidedProductBase providedProduct = providedProductBaseMapper.detailToDomain(detail);
		providedProduct.setProvidedProductBaseId(ProvidedProductBaseId.builder()
			.productBaseId(productBaseId)
			.businessCode(businessCode)
			.build());
		try {
			providedProduct = providedProductBaseService.update(providedProduct);
			return providedProductBaseMapper.domainToDetail(providedProduct);
		} catch (EntityNotFoundException exception) {
			throw new ResponseStatusException(ErrorCode.PRODUCT_NOT_FOUND.getCode(),
				ErrorCode.PRODUCT_NOT_FOUND.getMessage());
		}

	}
	//Remove Provided Product
	@DeleteMapping(Endpoint.Provider.ROOT + Endpoint.ProvidedProduct.DETAIL)
	@PreAuthorize("hasAuthority('ADMIN')")
	public void deleteProvidedProduct(
		@PathVariable("providerId") String businessCode,
		@PathVariable("productBaseId") Long productBaseId) {
		try {
			providedProductBaseService.delete(businessCode, productBaseId);
		} catch (EntityNotFoundException exception) {
			throw new ResponseStatusException(ErrorCode.PRODUCT_NOT_FOUND.getCode(),
				ErrorCode.PRODUCT_NOT_FOUND.getMessage());
		}
	}

	@GetMapping(Endpoint.Provider.ROOT + Endpoint.ProvidedProduct.DETAIL)
	public ProvidedProductBaseDetail getById(
		@PathVariable("providerId") String businessCode,
		@PathVariable("productBaseId") Long productBaseId) {
		try {
			ProvidedProductBase product = providedProductBaseService.getById(businessCode, productBaseId);
			return providedProductBaseMapper.domainToDetail(product);
		} catch (EntityNotFoundException exception) {
			throw new ResponseStatusException(HttpStatus.NOT_FOUND,
				ErrorCode.PRODUCT_NOT_FOUND.getMessage(),
				exception);
		}
	}
}