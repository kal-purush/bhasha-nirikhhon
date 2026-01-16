/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.project.ecampusconnect.sessionbeans;

/**
 *
 * @author Advait
 */
import java.util.ArrayList;

import javax.ejb.Stateful;

/**
 * Session Bean implementation class ShoppingCartBean
 */
@Stateful(name = "CourseCart")
public class CourseCartBean 
{
    public ArrayList<String> cartItems;

	public void addCourse(String course) {
		if (cartItems == null) {
			cartItems = new ArrayList<>();
		}
		cartItems.add(course);
	}
	
	public void removeCourse(String course) {
		cartItems.remove(course);
	}
	
	public void setCartItems(ArrayList<String> cartItems) {
		this.cartItems = cartItems;
	}
	
	public ArrayList<String> getCartItems() {
		return this.cartItems;
	}
    
}