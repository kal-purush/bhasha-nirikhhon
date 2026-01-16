package com.artiexh.api.service.impl;

import com.artiexh.api.exception.ErrorCode;
import com.artiexh.api.service.CollectionService;
import com.artiexh.data.jpa.entity.CollectionEntity;
import com.artiexh.data.jpa.entity.ProvidedProductBaseEntity;
import com.artiexh.data.jpa.entity.ProvidedProductBaseId;
import com.artiexh.data.jpa.repository.CollectionRepository;
import com.artiexh.data.jpa.repository.ProvidedProductBaseRepository;
import com.artiexh.model.domain.Collection;
import com.artiexh.model.domain.ProvidedProductBase;
import com.artiexh.model.domain.ProvidedProductType;
import com.artiexh.model.mapper.CollectionMapper;
import com.artiexh.model.mapper.ProvidedProductBaseMapper;
import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class CollectionServiceImpl implements CollectionService {
	private final ProvidedProductBaseRepository productRepository;
	private final CollectionRepository collectionRepository;
	private final ProvidedProductBaseMapper productMapper;
	private final CollectionMapper collectionMapper;
	@Override
	@Transactional
	public Collection create(Collection collection, String businessCode) {
		CollectionEntity collectionEntity = CollectionEntity.builder()
			.name(collection.getName())
			.businessCode(businessCode)
			.imageUrl(collection.getImageUrl())
			.priceAmount(collection.getPriceAmount())
			.build();
		collectionRepository.save(collectionEntity);

		Set<ProvidedProductBase> providedProductBases = collection.getProvidedProducts().stream().map(product -> {
			ProvidedProductBaseEntity entity;
			if (product.getId() != null) {
				entity = productRepository.findById(product.getId())
					.orElseThrow(EntityNotFoundException::new);

				if (!entity.getProvidedProductBaseId().getBusinessCode().equals(businessCode)){
					throw new IllegalArgumentException(ErrorCode.PROVIDED_PRODUCT_INVALID.getMessage() + entity.getId());
				}

				//Update type COLLECTION
				List<Byte> types = new ArrayList<>(Arrays.asList(entity.getTypes()));
				if (!types.contains(ProvidedProductType.COLLECTION.getByteValue())) {
					types.add(ProvidedProductType.COLLECTION.getByteValue());
					entity.setTypes(types.toArray(new Byte[]{}));
				}

				//Update collection
				entity.getCollections().add(collectionEntity);
			} else {
				productRepository.findByProvidedProductBaseId(ProvidedProductBaseId.builder()
					.productBaseId(product.getProvidedProductBaseId().getProductBaseId())
					.businessCode(businessCode)
					.build())
					.ifPresent(providedProduct -> {
					throw new IllegalArgumentException(ErrorCode.PRODUCT_EXISTED.getMessage() + providedProduct.getId());
				});
				entity = productMapper.domainToEntity(product);

				entity.setProvidedProductBaseId(ProvidedProductBaseId.builder()
					.businessCode(businessCode)
					.productBaseId(product.getProvidedProductBaseId().getProductBaseId())
					.build());
				entity.setTypes(new Byte[] {ProvidedProductType.COLLECTION.getByteValue()});

				//Mapping collection
				Set<CollectionEntity> collections = new HashSet<>();
				collections.add(collectionEntity);
				entity.setCollections(collections);
			}
			productRepository.save(entity);
			return productMapper.entityToDomain(entity);
		}).collect(Collectors.toSet());

		collection.setId(collectionEntity.getId());
		collection.setName(collectionEntity.getName());
		collection.setImageUrl(collectionEntity.getImageUrl());
		collection.setPriceAmount(collection.getPriceAmount());
		collection.setProvidedProducts(providedProductBases);

		return collection;
	}

	@Override
	public Page<Collection> get(Specification<CollectionEntity> specification, Pageable pageable) {
		Page<CollectionEntity> collections = collectionRepository.findAll(specification,pageable);
		return collections.map(collectionMapper::entityToDomain);
	}
}