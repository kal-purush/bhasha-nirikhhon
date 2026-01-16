package com.ssh.service.practice.controller;

import java.util.ArrayList;
import java.util.List;

public class ParamTest {
	public static void main(String[] args){
		ParamTest paramTest = new ParamTest();
		Integer init_value = 0;
		System.out.println("change"+paramTest.changeInt(init_value));
		System.out.println("init_value"+init_value);

		List<Integer> a = new ArrayList<>();
		a.add(0, 1);
		System.out.println("change"+paramTest.changeInt(a));
		System.out.println("init_value"+a);
	}

	private Integer changeInt(Integer a){
		a = 10;
		return 9;
	}

	private List changeInt(List a){
		a.add(1, 10);
		return a;
	}
}