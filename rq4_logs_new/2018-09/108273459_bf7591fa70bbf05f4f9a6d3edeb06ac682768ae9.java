package com.wedeploy.android.query.aggregation;

import com.wedeploy.android.query.filter.BucketOrder;
import com.wedeploy.android.transport.Request;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Abstract class that represents a {@link Aggregation} that supports bucket
 * order.
 *
 * @author Victor Oliveira
 */
public class BucketOrderAggregation extends Aggregation {

	BucketOrderAggregation(String name, String field, String operator) {
		super(name, field, operator);
	}

	BucketOrderAggregation(String name, String field, String operator,
		Object value) {
		super(name, field, operator, value);
	}

	BucketOrderAggregation(String name, String field, String operator,
		BucketOrder... bucketOrder) {
		super(name, field, operator);

		this.bucketOrders = new ArrayList<>(Arrays.asList(bucketOrder));
	}

	BucketOrderAggregation(String name, String field, String operator,
		Object value, BucketOrder... bucketOrder) {
		super(name, field, operator, value);

		this.bucketOrders = new ArrayList<>(Arrays.asList(bucketOrder));
	}

	BucketOrderAggregation(String name, String field, String operator,
		Object value, Integer size, BucketOrder... bucketOrder) {
		super(name, field, operator, value);

		this.size = size;
		this.bucketOrders = new ArrayList<>(Arrays.asList(bucketOrder));
	}

	/**
	 * Adds {@link BucketOrder} instance to the current Aggregation.
	 *
	 * @param bucketOrder The bucket order to be added to this aggregation
	 * @return {@link BucketOrderAggregation}
	 */
	public BucketOrderAggregation addBucketOrder(BucketOrder... bucketOrder) {
		if (bucketOrder == null) {
			return this;
		}

		if (this.bucketOrders == null) {
			this.bucketOrders = new ArrayList<>();
		}

		this.bucketOrders.addAll(Arrays.asList(bucketOrder));

		return this;
	}

	/**
	 * Gets a map that represents the {@link Request} body for this
	 * {@link Aggregation}.
	 *
	 * @return {@link Map}
	 */
	@Override
	public Map body() {
		Map body = super.body();
		Map fieldMap = (Map) body.get(getField());

		if (size != null) {
			fieldMap.put("size", size);
		}

		if (bucketOrders != null && !bucketOrders.isEmpty()) {
			fieldMap.put("order", bucketOrders);
		}

		return body;
	}

	public Integer getSize() {
		return size;
	}

	public BucketOrderAggregation setSize(Integer size) {
		this.size = size;
		return this;
	}

	private Integer size;
	private List<BucketOrder> bucketOrders;
}