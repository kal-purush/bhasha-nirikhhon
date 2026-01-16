package com.chris.oscjp.chapter5;

public class Array2D {

	static final int array_size = 10;
	public static void main(String[] args) {
		
		int [][] Arr2D = new int[array_size][array_size];
		
		for(int i = 0; i < array_size; i++){
			Arr2D [i][i] = i;
			if(i == 8){
				System.out.println("i = 8 break - should exit ");
				break;
			}
			if(i == 5){
				System.out.println("i = 5 continue - should skip 5 ");
				continue;
			}
			System.out.println("Arr2D [i][i] = "  + Arr2D [0][i]);
			for(int j = 0; j < array_size; j++){
				Arr2D [i][j] = j;
				if(j == 7){
					System.out.println("j = 7 continue - should skip 7 ");
					continue;
				}
				System.out.println("Arr2D [i][j] = "  + Arr2D [i][j]);
			}
		}

	}

}