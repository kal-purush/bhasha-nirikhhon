using System.Collections;
using System.Collections.Generic;
using TMPro;
using UnityEngine;
using UnityEngine.UI;

public class TrashProgress : MonoBehaviour
{
    [SerializeField] private GameObject itemPrefab;
    [SerializeField] private Transform itemParent;
    [SerializeField] private Image progressBar;

    private int totalScore = 0;
    private int currentScore = 0;

    private Dictionary<Trash.TrashType, int> trashTypePair = new Dictionary<Trash.TrashType, int>();

    public void CreateItems(List<TrashItem> trashItems)
    {
        List<Trash.TrashType> trashTypes = new List<Trash.TrashType>();

        foreach(TrashItem trashItem in trashItems)
        {
            if (trashTypePair.ContainsKey(trashItem.trash.trashType))
            {
                trashTypePair[trashItem.trash.trashType] += 1;
            }
            else
            {
                trashTypePair.Add(trashItem.trash.trashType, 1);
            }

            if (!trashTypes.Contains(trashItem.trash.trashType))
            {
                trashTypes.Add(trashItem.trash.trashType);

                GameObject item = Instantiate(itemPrefab, itemParent);

                ItemData itemData = item.GetComponent<ItemData>();
                itemData.icon.sprite = trashItem.icon;
                itemData.scoreText.text = trashTypePair[trashItem.trash.trashType].ToString();
            }

            totalScore += trashItem.trash.Score;
        }

        Debug.Log(totalScore);
    }

    public void AddScore(Trash trash)
    {
        currentScore += trash.Score;

        progressBar.fillAmount = currentScore / (float)totalScore;

        if (trashTypePair.ContainsKey(trash.trashType))
        {
            trashTypePair[trash.trashType] -= 1;
        }

        Debug.Log("Score is now: " + currentScore);
    }
}