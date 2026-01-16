package com.artiexh.api.controller;

import com.artiexh.api.base.common.Endpoint;
import com.artiexh.api.service.ShopService;
import com.artiexh.model.domain.Shop;
import com.artiexh.model.rest.PageResponse;
import com.artiexh.model.rest.PaginationAndSortingRequest;
import com.artiexh.model.rest.address.AddressResponse;
import com.artiexh.model.rest.shop.ShopPageFilter;
import jakarta.persistence.EntityNotFoundException;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springdoc.core.annotations.ParameterObject;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping(Endpoint.Shop.ROOT)
@RequiredArgsConstructor
public class ShopController {
	private final ShopService shopService;

	@GetMapping
	public PageResponse<Shop> getAllShops(
		@ParameterObject @Valid ShopPageFilter filter,
		@ParameterObject @Valid PaginationAndSortingRequest paginationAndSortingRequest
	) {
		return new PageResponse<>(
			shopService.getShopInPage(filter.getSpecification(), paginationAndSortingRequest.getPageable())
		);
	}

	@GetMapping("/{id}")
	public Shop getShop(@PathVariable Long id) {
		try {
			return shopService.getShopById(id);
		} catch (EntityNotFoundException ex) {
			throw new ResponseStatusException(HttpStatus.NOT_FOUND, ex.getMessage());
		}
	}

	@GetMapping("/{id}" + Endpoint.Shop.ADDRESS)
	public AddressResponse getShopAddress(@PathVariable Long id) {
		try {
			return shopService.getShopAddress(id);
		} catch (EntityNotFoundException ex) {
			throw new ResponseStatusException(HttpStatus.NOT_FOUND, ex.getMessage());
		}
	}

}