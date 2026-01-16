package com.artiexh.api.controller.provider;

import com.artiexh.api.base.common.Endpoint;
import com.artiexh.api.service.provider.ProviderCategoryService;
import com.artiexh.model.domain.ProviderCategory;
import com.artiexh.model.rest.PageResponse;
import com.artiexh.model.rest.PaginationAndSortingRequest;
import jakarta.persistence.EntityNotFoundException;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springdoc.core.annotations.ParameterObject;
import org.springframework.data.domain.Page;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping(Endpoint.ProviderCategory.ROOT)
@RequiredArgsConstructor
public class ProviderCategoryController {
	private final ProviderCategoryService providerCategoryService;

	@GetMapping
	public PageResponse<ProviderCategory> getAll(@ParameterObject PaginationAndSortingRequest paginationAndSortingRequest,
												 @RequestParam(required = false) String name) {
		Page<ProviderCategory> providerCategories = providerCategoryService.getAll(
			name,
			paginationAndSortingRequest.getPageable()
		);
		return new PageResponse<>(providerCategories);
	}

	@PostMapping
	@PreAuthorize("hasAnyAuthority('ADMIN','STAFF')")
	public ProviderCategory create(@Valid @RequestBody ProviderCategory providerCategory) {
		return providerCategoryService.create(providerCategory);
	}

	@PutMapping("/{id}")
	@PreAuthorize("hasAnyAuthority('ADMIN','STAFF')")
	public ProviderCategory update(@PathVariable("id") Long id,
								   @Validated @RequestBody ProviderCategory providerCategory) {
		providerCategory.setId(id);
		try {
			return providerCategoryService.update(providerCategory);
		} catch (EntityNotFoundException ex) {
			throw new ResponseStatusException(HttpStatus.NOT_FOUND, ex.getMessage(), ex);
		}
	}

	@DeleteMapping("/{id}")
	@PreAuthorize("hasAnyAuthority('ADMIN','STAFF')")
	public void delete(@PathVariable Long id) {
		try {
			providerCategoryService.delete(id);
		} catch (EntityNotFoundException ex) {
			throw new ResponseStatusException(HttpStatus.NOT_FOUND, ex.getMessage(), ex);
		}
	}
}