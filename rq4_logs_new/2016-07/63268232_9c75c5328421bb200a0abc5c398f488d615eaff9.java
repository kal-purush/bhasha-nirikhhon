package com.sergtm.application.findMaxNumber;


public class FindMaxNumber {
    public int findMaxNum(int[][] matrix,boolean[][] availableElements){
        int resultNumber = 0;
        for(int col = 0;col < matrix.length;col++){
            int index = matrix[col].length;
            for(int row = 0;row < index ;row++){
                int element = matrix[col][row];
                boolean available = availableElements[col][row];
                if(available && element > resultNumber){
                    resultNumber = element;
                }
            }
        }
        return resultNumber;
    }
}