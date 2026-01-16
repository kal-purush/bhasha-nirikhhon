package com.artiexh.api.controller.address;

import com.artiexh.api.base.common.Endpoint;
import com.artiexh.api.service.AddressService;
import com.artiexh.model.domain.Country;
import com.artiexh.model.domain.District;
import com.artiexh.model.domain.Province;
import com.artiexh.model.domain.Ward;
import com.artiexh.model.rest.PageResponse;
import com.artiexh.model.rest.PaginationAndSortingRequest;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springdoc.core.annotations.ParameterObject;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RequiredArgsConstructor
@RestController
@RequestMapping(path = Endpoint.Address.ROOT)
public class AddressController {
	private final AddressService addressService;

	@GetMapping(Endpoint.Address.COUNTRY)
	public PageResponse<Country> getCountries(@ParameterObject @Valid PaginationAndSortingRequest paginationAndSortingRequest) {
		var countryPage = addressService.getCountries(paginationAndSortingRequest.getPageable());
		return new PageResponse<>(countryPage);
	}

	@GetMapping(Endpoint.Address.COUNTRY + "/{countryId}" + Endpoint.Address.PROVINCE)
	public PageResponse<Province> getProvinces(@PathVariable Short countryId,
											   @ParameterObject @Valid PaginationAndSortingRequest paginationAndSortingRequest) {
		var provincePage = addressService.getProvinces(countryId, paginationAndSortingRequest.getPageable());
		return new PageResponse<>(provincePage);
	}

	@GetMapping(Endpoint.Address.PROVINCE + "/{provinceId}" + Endpoint.Address.DISTRICT)
	public PageResponse<District> getDistricts(@PathVariable Short provinceId,
											   @ParameterObject @Valid PaginationAndSortingRequest paginationAndSortingRequest) {
		var districtPage = addressService.getDistricts(provinceId, paginationAndSortingRequest.getPageable());
		return new PageResponse<>(districtPage);
	}

	@GetMapping(Endpoint.Address.DISTRICT + "/{districtId}" + Endpoint.Address.WARD)
	public PageResponse<Ward> getWards(@PathVariable Short districtId,
									   @ParameterObject @Valid PaginationAndSortingRequest paginationAndSortingRequest) {
		var wardPage = addressService.getWards(districtId, paginationAndSortingRequest.getPageable());
		return new PageResponse<>(wardPage);
	}

}