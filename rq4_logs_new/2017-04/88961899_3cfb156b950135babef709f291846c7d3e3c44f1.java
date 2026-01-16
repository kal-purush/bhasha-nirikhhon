package de.hsbochum.fbg.kswe;

import java.util.Arrays;
import java.util.List;

public class Java8Test {
	
	public static void main(String[] args) {
		List<String> testList = Arrays.asList(new String[] {"a", "b", "c"});
		
		testList.stream().forEach(s -> {
			System.out.print(s);
		});
	}
	
}