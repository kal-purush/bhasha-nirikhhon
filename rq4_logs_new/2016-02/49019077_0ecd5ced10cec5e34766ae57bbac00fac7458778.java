package com.kupybaton.web.filter;

public class DataValidator {
	
	public static boolean validatePurchaseCreate (String productId, String quantity) {
		try {
			if (productId != null && Integer.valueOf(productId) instanceof Integer && quantity.length()>0 && Double.valueOf(quantity) instanceof Double ) {
				return true;
			}
				
		} catch (NumberFormatException e) {
			e.printStackTrace();
			return false;
		}
		return false;
	}
	
	public static boolean validateName (String name) {
			if (name.length()>0) {
				return true;
			}
			
		return false;
	}

}