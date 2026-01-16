package com.artiexh.api.controller;

import com.artiexh.api.base.common.Endpoint;
import com.artiexh.api.exception.ErrorCode;
import com.artiexh.api.service.InventoryService;
import com.artiexh.model.domain.Collection;
import com.artiexh.model.domain.InventoryItem;
import com.artiexh.model.mapper.InventoryMapper;
import com.artiexh.model.rest.PageResponse;
import com.artiexh.model.rest.PaginationAndSortingRequest;
import com.artiexh.model.rest.collection.CollectionDetail;
import com.artiexh.model.rest.inventory.InventoryItemDetail;
import com.artiexh.model.rest.inventory.ItemFilter;
import jakarta.persistence.EntityNotFoundException;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springdoc.core.annotations.ParameterObject;
import org.springframework.data.domain.Page;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequiredArgsConstructor
@RequestMapping(path = Endpoint.InventoryItem.ROOT)
public class InventoryItemController {
	private final InventoryService inventoryService;
	private final InventoryMapper inventoryMapper;
	@PostMapping()
	@PreAuthorize("hasAuthority('ARTIST')")
	public InventoryItemDetail saveItem(
		Authentication authentication,
		@Valid @RequestBody InventoryItemDetail detail) {
		try {
			long userId = (long) authentication.getPrincipal();
			detail.setArtistId(userId);

			InventoryItem item = inventoryMapper.detailToDomain(detail);
			item = inventoryService.save(item);

			return inventoryMapper.domainToDetail(item);
		} catch (EntityNotFoundException exception) {
			throw new ResponseStatusException(HttpStatus.NOT_FOUND,
				ErrorCode.PRODUCT_NOT_FOUND.getMessage(),
				exception);
		} catch (IllegalArgumentException exception) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
				exception.getMessage(),
				exception);
		}
	}

	@GetMapping()
	@PreAuthorize("hasAuthority('ARTIST')")
	public PageResponse<InventoryItemDetail> getAll(
		Authentication authentication,
		@ParameterObject ItemFilter filter,
		@Valid @ParameterObject PaginationAndSortingRequest paginationAndSortingRequest) {
		try {
			long userId = (long) authentication.getPrincipal();
			filter.setArtistId(userId);
			Page<InventoryItem> itemPage = inventoryService.getAll(filter.getSpecification(), paginationAndSortingRequest.getPageable());

			return new PageResponse<>(itemPage.map(inventoryMapper::domainToDetail));
		} catch (EntityNotFoundException exception) {
			throw new ResponseStatusException(HttpStatus.NOT_FOUND,
				ErrorCode.PRODUCT_NOT_FOUND.getMessage(),
				exception);
		} catch (IllegalArgumentException exception) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
				exception.getMessage(),
				exception);
		}
	}
}