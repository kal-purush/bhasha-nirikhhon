using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EveryLevel : MonoBehaviour
{
    public MapNode[] nodes;
    [SerializeField] int oneCellWidth;
    [SerializeField] int oneCellHeight;
    [SerializeField] int padding;

    public void SetRoomsPosition(System.Random random)
    {
        for (int i = 0; i < nodes.Length; i++)
        {
            if (nodes[i] != null && nodes[i].isUsed)
            {
                int x = random.Next(padding, oneCellWidth - padding);
                int y = random.Next(padding, oneCellHeight - padding) + i * oneCellHeight;
                nodes[i].gameObject.GetComponent<RectTransform>().anchoredPosition = new Vector2(x,y);
            }
        }
    }
}