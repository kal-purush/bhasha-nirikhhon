package aa;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 * 100@77,20@78,50@77,200@79,300@77,1024@92,500@76
 *
 */
public class Demo {
	public static void main(String[] args) {
		StringBuilder sb = new StringBuilder();
		String str = "100@77,20@78,50@77,200@79,300@77,1024@92,500@76";
		String[] split = str.split(",");
		for (String string : split) {
			String[] split2 = string.split("@");
			String string2 = Arrays.toString(split2);
			System.out.println(string2);
		}
	}

}