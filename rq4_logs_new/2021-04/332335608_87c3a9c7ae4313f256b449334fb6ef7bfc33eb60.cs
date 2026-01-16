using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System;

public class PatternMatchingAbstraction
{

    //***** Variables Start *****\\
    private int level = 1; //Player's level min: 1, max: 3
    private int[] patternMap = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}; //values of the buttons -> 0: closed, 1: opened
    //***** Variables End *****\\


    //***** Getter Functions Start *****\\
    
    /**
    *  @brief getter function for level
    *  @returns level type of int, current level
    */
    public int getLevel(){
        return level;
    }

    /**
    *  @brief getter function for patternMap
    *  @returns patternMap type of int[], current map with button values
    */
    public int[] getPatternMap(){
        return patternMap;
    }

    //***** Getter Functions End *****\\

    //***** Setter Functions Start *****\\

    /**
    *  @brief setter function to change the level
    *  @param newLevel type of int, new level
    */
    public void setLevel(int newLevel){
        level = newLevel;
    }

    /**
    *  @brief setter function to change the patternMap
    *  @param newPatternMap type of int[], new values of the buttons
    */
    public void setPatternMap(int[] newPatternMap){
        patternMap = newPatternMap;
    }

    /**
    *  @brief function to generate new pattern
    */
    public void generatePattern(){
        System.Random r = new System.Random();
        if(level == 1){
            for(int i = 0; i < 5; i++){
                int random = r.Next(0,15);
                Debug.Log("Random number is:" + random);
                //Open the index of the random button to create the pattern
                patternMap[random] = 1;
            }
        }
        // }else if(level == 2){

        // }else if(level == 3){

        // }
    }

    //***** Setter Functions End *****\\

    //***** Helper Functions Start *****\\

    /**
    *  @brief helper function to reset pattern map to a position in which all buttons are closed
    */
    //TODO: Might need to make it public if I need to access it from the controller class as well
    private void resetPatternMap(){
        for(int i = 0; i < patternMap.Length; i++){
            patternMap[i] = 0;
        }
    }

    //***** Helper Functions End *****\\


    // // Start is called before the first frame update
    // void Start()
    // {
        
    // }

    // // Update is called once per frame
    // void Update()
    // {
        
    // }
}