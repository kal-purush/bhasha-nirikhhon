using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class QuestManager : MonoBehaviour
{
    public RecipeDatabase recipeDatabase;
    public QuestCardManager questCardManager;

    private Queue<Recipe> questQueue = new Queue<Recipe>();
    private int currentQuestIndex = 0;

    void Start()
    {
        GenerateQuests();
    }

    public void StartGame()
    {
        StartNextQuest();
    }

    void GenerateQuests()
    {
        List<Recipe> firstRecipes = recipeDatabase.recipes.FindAll(r => r.category == Recipe.Category.First);
        List<Recipe> secondRecipes = recipeDatabase.recipes.FindAll(r => r.category == Recipe.Category.Second);
        List<Recipe> finalRecipes = recipeDatabase.recipes.FindAll(r => r.category == Recipe.Category.Final);

        AddRandomRecipesToQueue(firstRecipes, 3);
        AddRandomRecipesToQueue(secondRecipes, 3);
        AddRandomRecipesToQueue(finalRecipes, 1);
    }

    void AddRandomRecipesToQueue(List<Recipe> recipes, int count)
    {
        for (int i = 0; i < count; i++)
        {
            int index = Random.Range(0, recipes.Count);
            questQueue.Enqueue(recipes[index]);
            recipes.RemoveAt(index);
        }
    }

    public void StartNextQuest()
    {
        if (questQueue.Count > 0)
        {
            Recipe currentQuest = questQueue.Dequeue();

            questCardManager.CreateActiveQuestCard(currentQuest);
            currentQuestIndex++;
        }
        else
        {
            Debug.Log("End game");

        }
    }

    public void OnQuestCompleted()
    {
        StartNextQuest();
    }

}