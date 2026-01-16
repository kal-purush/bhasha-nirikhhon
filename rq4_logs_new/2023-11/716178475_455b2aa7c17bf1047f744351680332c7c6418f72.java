package com.se.fit.TravelProject.entities;

public class CartItem {
	 private Object cartItem;
	    private int quantity;
		public CartItem(Object cartItem, int quantity) {
			super();
			this.cartItem = cartItem;
			this.quantity = quantity;
		}
		public Object getCartItem() {
			return cartItem;
		}
		public void setCartItem(Object cartItem) {
			this.cartItem = cartItem;
		}
		public int getQuantity() {
			return quantity;
		}
		public void setQuantity(int quantity) {
			this.quantity = quantity;
		}
	    
	    public void increaseQuantity() {
	        this.quantity++;
	    }

	    public void decreaseQuantity() {
	        if (this.quantity > 0) {
	            this.quantity--;
	        }
	    } public boolean isTour() {
	        return cartItem instanceof Tour;
	    }

	    public boolean isComboTour() {
	        return cartItem instanceof Combo;
	    }
	    public int getID() {
	        if (isTour()) {
	            return ((Tour) cartItem).getTravelPackageId();
	        } else if (isComboTour()) {
	            return ((Combo) cartItem).getTravelPackageId();
	        }
	        return 0;
	    }
}