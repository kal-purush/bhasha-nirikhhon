package com.artiexh.model.rest.productvariant.request;

import com.artiexh.model.rest.productvariant.ProductVariantDetail;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Set;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class CreateProductVariantDetail {
	@JsonSerialize(using = ToStringSerializer.class)
	@NotNull
	private Long productBaseId;

	@Valid
	@NotEmpty
	private Set<ProductVariantDetail> variants;
}