package org.example.model.repository.invoice;

import org.bson.codecs.pojo.annotations.BsonId;

public class InvoiceDocument {
	
	@BsonId
	private int id;
	private double totalSales;
	
}