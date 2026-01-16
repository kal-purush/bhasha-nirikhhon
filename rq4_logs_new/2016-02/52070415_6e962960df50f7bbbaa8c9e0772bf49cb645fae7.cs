using UnityEngine;
using System.Collections;
using System.Collections.Generic;
using System;
using System.Linq;

public class AnswerLibrary : MonoBehaviour 
{
    
//    public Jawi[] setQuestion;
    public QuestionLibrary.Category[] setQuestion;

    public GameObject questionGameObject;
    private QuestionLibrary questionLib;
	
    void Awake()
    {
        questionLib = questionGameObject.GetComponent<QuestionLibrary>();

//        LoadCategory(PlayerPrefs.GetInt("Category"));
        LoadCategoryLevel(PlayerPrefs.GetString("Level Name"));
    }



    public void LoadCategory(int id)
    {
        QuestionLibrary.Category[] level;

        switch (id)
        {
            case 1: // Animal
                level = questionLib.animal1;
                break;
            case 2: // Family     
                level = questionLib.family;
                break;
            case 3: // Food
                level = questionLib.food;
                break;
            case 4: // ibadah
                level = questionLib.ibadah;
                break;

            default:
                level = questionLib.animal1;
                break;
        }


        setQuestion = new QuestionLibrary.Category[level.Length];

        // Initialize data-data for answer for each question
        for (int i = 0; i < level.Length; i++)
        {
            level[i].InitData();
        }

        Array.Copy(level, setQuestion, level.Length);

    }

    public void LoadCategoryLevel(string name)
    {
        QuestionLibrary.Category[] level;

        switch (name)
        {
            //---------
            // Animal
            //---------
            case "animal1": // Animal
                level = questionLib.animal1;
                break;
            case "animal2": // Animal
                level = questionLib.animal2;
                break;
            case "animal3": // Animal
                level = questionLib.animal3;
                break;
            //---------
            // Family
            //---------
            case "family": // Family     
                level = questionLib.family;
                break;
            //---------
            // Food
            //---------
            case "food": // Food
                level = questionLib.food;
                break;
            //---------
            // Ibadah
            //---------
            case "ibadah": // ibadah
                level = questionLib.ibadah;
                break;

            default:
                level = questionLib.animal1;
                break;
        }


        setQuestion = new QuestionLibrary.Category[level.Length];

        // Initialize data-data for answer for each question in selected level
        for (int i = 0; i < level.Length; i++)
        {
            level[i].InitData();
        }

        // Copy an array to set of question
        Array.Copy(level, setQuestion, level.Length);

    }
                
}