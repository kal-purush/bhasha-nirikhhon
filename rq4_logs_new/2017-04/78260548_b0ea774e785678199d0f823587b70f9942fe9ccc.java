package com.studyguide.excercise;

public class Occurences {

	public static void main(String[] args) {
		String value = "i am super smart, right?";
		char[] charArray = value.toCharArray();
		
		for(int i=0; i<charArray.length; i++)
		{
			char charToSearch = charArray[i];
			int count = 0;
			for(int j=0; j<charArray.length; j++)
			{
				if(charToSearch == charArray[j])
				{
					charArray[j] = ' ';
					count++;
				}
			}
			if(charToSearch != ' ') System.out.println(charToSearch+" is occurenced "+count+" times");
		}

	}

}