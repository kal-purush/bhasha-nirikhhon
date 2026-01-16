package com.artiexh.api.controller;

import com.artiexh.api.base.common.Endpoint;
import com.artiexh.api.exception.ErrorCode;
import com.artiexh.api.service.CollectionService;
import com.artiexh.model.domain.Collection;
import com.artiexh.model.mapper.CollectionMapper;
import com.artiexh.model.rest.PageResponse;
import com.artiexh.model.rest.PaginationAndSortingRequest;
import com.artiexh.model.rest.collection.CollectionDetail;
import com.artiexh.model.rest.provider.filter.CollectionFilter;
import jakarta.persistence.EntityNotFoundException;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springdoc.core.annotations.ParameterObject;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequiredArgsConstructor
@RequestMapping(path = Endpoint.Collection.ROOT)
public class CollectionController {
	private final CollectionMapper collectionMapper;
	private final CollectionService collectionService;
	@PostMapping()
	@PreAuthorize("hasAuthority('ARTIST')")
	public CollectionDetail create(
		Authentication authentication,
		@Valid @RequestBody CollectionDetail detail) {
		Collection collection = collectionMapper.detailToDomain(detail);
		long userId = (long) authentication.getPrincipal();
		collection.setArtistId(userId);
		try {
			collection = collectionService.create(collection);
			return collectionMapper.domainToDetail(collection);
		} catch (EntityNotFoundException exception){
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
	public PageResponse<CollectionDetail> getAll(
		Authentication authentication,
		@Valid @ParameterObject PaginationAndSortingRequest paginationAndSortingRequest) {
		long userId = (long) authentication.getPrincipal();
		CollectionFilter filter = CollectionFilter.builder().artistId(userId).build();
		return new PageResponse<>(
			collectionService.getAll(filter.getSpecification(), paginationAndSortingRequest.getPageable())
				.map(collectionMapper::domainToDetail)
		);
	}
}