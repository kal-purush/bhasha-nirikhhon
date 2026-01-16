package desencryptedchat;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Tilley
 */

import java.util.Arrays; 
public class KeyGenerator {
    public static int INITIALKEYLENGTH = 64;
    public static int PERMUTEDKEYLENGTH1 = 56;
    public static int PERMUTEDKEYLENGTH2 = 48;
    public static int NUMBEROFROUNDS = 16;
    
    
//    private static String key =
//            "00010011"
//            + "00110100"
//            + "01010111"
//            + "01111001"
//            + "10011011"
//            + "10111100"
//            + "11011111"
//            + "11110001";
    // 0001001100110100010101110111100110011011101111001101111111110001
    
    private static String key = "";
    
    
    private static int[] PC1 = 
	{  
         57, 49, 41, 33, 25, 17, 9,
         1, 58, 50, 42, 34, 26, 18,
         10,  2, 59, 51, 43, 35, 27,
         19, 11,  3, 60, 52, 44, 36,
         63, 55, 47, 39, 31, 23, 15,
         7, 62, 54, 46, 38, 30, 22,
         14,  6, 61, 53, 45, 37, 29,
         21, 13,  5, 28, 20, 12, 4
                
	};
    private static int[] lShifts =
        {
            1,  1,  2,  2,  2,  2,  2,  2,  1,  2,  2,  2,  2,  2,  2,  1
	};
    
    private static int[] PC2 = 
	{
	 14, 17, 11, 24,  1,  5,
         3, 28, 15,  6, 21, 10,
         23, 19, 12,  4, 26, 8,
         16,  7, 27, 20, 13, 2,
         41, 52, 31, 37, 47, 55,
         30, 40, 51, 45, 33, 48,
         44, 49, 39, 56, 34, 53,
         46, 42, 50, 36, 29, 32
	};
    
    public KeyGenerator(String privateKey){
        key = privateKey;
    }
    
    // performs PC1 permutation on intial 64 bit key
    // outputs 56 bit permutation key for use with lshifts
    public static String initialKeyToPC1(String startingKey){
        
        String[] initialKey = startingKey.split("");
        String[] permutedKey1arr = new String[PERMUTEDKEYLENGTH1];
        for (int i = 0 ; i < PERMUTEDKEYLENGTH1 ; i++){
            permutedKey1arr[i] = initialKey[PC1[i] - 1];

        }
        String permutedKey1 = String.join("", permutedKey1arr);
//        System.out.println("permuted key 1: " + permutedKey1);        

        return permutedKey1;
        
    }
    
    //takes a 28 bit l or r half of pk1, outputs a half of a 56 bit round key
    // output moves on to be combined with other half in PC2
    public static String leftShift(String half, int round){
        
        String shiftedHalf = half.substring(lShifts[round]) + half.substring(0, lShifts[round]); 
//        System.out.println(shiftedHalf);
        return shiftedHalf;
    }
    
    // concatenates both half of shifted round key, makes 56 bit new key
    // sends new key through PC2 to create the finial round key
    public static String shiftedHalvesToPC2(String lhalf, String rhalf){
        
        String concatenatedHalves = lhalf + rhalf;
        String[] initialKey = concatenatedHalves.split("");
        String[] permutedKey2arr = new String[PERMUTEDKEYLENGTH2];
        
        for (int i = 0 ; i < PERMUTEDKEYLENGTH2 ; i++){
            
            permutedKey2arr[i] = initialKey[PC2[i] - 1];
//            System.out.println(PC2[i]);
        }
        String roundKey = String.join("", permutedKey2arr);
        return roundKey;
        
    }
    // Does the process of round key generation
    // outputs an array of round keys
    public static String[] keyGenerator(String key64bit){
        
        String permutationKey = initialKeyToPC1(key64bit);
        int middleOfString = permutationKey.length() / 2;
        
        String[] RoundKeyArray = new String[NUMBEROFROUNDS];
        
        for (int round = 0; round < NUMBEROFROUNDS; round++){
            String[] keyHalves = {permutationKey.substring(0, middleOfString),
                permutationKey.substring(middleOfString)};
            
            String lHalf = keyHalves[0];
            String rHalf = keyHalves[1];
            
            String shiftedLHalf = leftShift(lHalf,round);
            String shiftedRHalf = leftShift(rHalf,round);
//            System.out.println(shiftedRHalf.length());
            
            permutationKey = shiftedLHalf + shiftedRHalf;
//            System.out.println(permutationKey.length());
            
            String roundKey = shiftedHalvesToPC2(shiftedLHalf,shiftedRHalf);
            RoundKeyArray[round] = roundKey;
        }
        return RoundKeyArray;
    }
    
    public static String[] roundKeyArrayReversal(String[] RoundKeyArray){
        String[] ReversedRoundKeyArray = RoundKeyArray;
        for(int i = 0; i < ReversedRoundKeyArray.length / 2; i++){
            String temp = ReversedRoundKeyArray[i];
            ReversedRoundKeyArray[i] = ReversedRoundKeyArray[ReversedRoundKeyArray.length -i -1];
            ReversedRoundKeyArray[ReversedRoundKeyArray.length -i -1] = temp;
        }
        return ReversedRoundKeyArray;
    }
    
    
    public static String getKey(){
        return key;
    }
    
    public static void setKey(String userKey){
        key = userKey;
    }

 
}