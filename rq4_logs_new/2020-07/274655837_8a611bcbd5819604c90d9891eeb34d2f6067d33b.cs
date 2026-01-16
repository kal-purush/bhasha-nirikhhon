using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ScreenFix : MonoBehaviour
{
    public List<BoxCollider2D> borders;
    public List<EdgeCollider2D> stringColliders;
    public List<BoxCollider2D> strings;
    public List<BoxCollider2D> borderHolders;

    // Start is called before the first frame update
    void Start()
    {
        if (Screen.height / (Screen.width / 9) > 16)
        {
            for (int i = 0; i < borders.Count; ++i)
                ScreenOptimization.fix18_9(borders[i]);

            for (int i = 0; i < stringColliders.Count; ++i)
                ScreenOptimization.fix18_9(stringColliders[i]);

            for (int i = 0; i < strings.Count; ++i)
                ScreenOptimization.fix18_9(strings[i]);

            for (int i = 0; i < borderHolders.Count; ++i)
                ScreenOptimization.fix18_9(borderHolders[i]);
        }
    }

}