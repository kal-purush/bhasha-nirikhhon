/**
 * @author Kevin Bohinski <bohinsk1@tcnj.edu>
 * @version 1.0
 * @since 2015-10-14
 *
 * Project Name:  HashMap
 * Description:   Custom HashMap implementation.
 *
 * Filename:      Driver.java
 * Description:   Driver class used for testing.
 * Last Modified: 2015-10-14
 *
 * Copyright (c) 2015 Kevin Bohinski. All rights reserved.
 */

/* Setting Package */
package com.kevinbohinski.algs;

public class Driver {

	/**
	 * Main method, runs some tests of a HashMap.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		HashMap<String, Integer> hm = new HashMap<String, Integer>(50);
		System.out.println("Setting a to 10    : " + hm.set("a", 10));
		System.out.println("Getting a          : " + hm.get("a"));
		System.out.println("--------------------------");
		System.out.println("Setting b to 50    : " + hm.set("b", 50));
		System.out.println("Getting b          : " + hm.get("b"));
		System.out.println("--------------------------");
		System.out.println("Overwriting a to 20: " + hm.set("a", 20));
		System.out.println("Getting a          : " + hm.get("a"));
		System.out.println("Deleting a         : " + hm.delete("a"));
		System.out.println("Getting a          : " + hm.get("A"));
		System.out.println("--------------------------");
		System.out.println("Deleting c         : " + hm.delete("c"));
		System.out.println("Setting c to 15    : " + hm.set("c", 15));
		System.out.println("Getting c          : " + hm.get("c"));
		System.out.println("--------------------------");
		System.out.println("Setting null to 15 : " + hm.set(null, 15));
		System.out.println("Getting null       : " + hm.get(null));
		System.out.println("--------------------------");
		System.out.println("Getting a          : " + hm.get("a"));
		System.out.println("Getting b          : " + hm.get("b"));
		System.out.println("Getting c          : " + hm.get("c"));
		System.out.println("--------------------------");
		System.out.println("Getting load factor: " + hm.load());
	}

}