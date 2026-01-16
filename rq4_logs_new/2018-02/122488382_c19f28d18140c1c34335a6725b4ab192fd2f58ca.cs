using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class UIFieldClassObject : MonoBehaviour
{
    [SerializeField] GameObject uiGroup;
    [SerializeField] GameObject uiClassFieldContentPrefab;
    enum FieldStats
    {
        Close,
        Open,
    }
    FieldStats fieldState = FieldStats.Close;
    public void OnClickClassName()
    {
        switch (fieldState)
        {
            case FieldStats.Close:
                CreateOpenField();
                fieldState = FieldStats.Open;
                break;
            case FieldStats.Open:
                DestoryCloseField();
                fieldState = FieldStats.Close;
                break;
        }

    }
    void CreateOpenField()
    {
        var i = 3;
        while (i > 0)
        {
            GameObject.Instantiate(uiClassFieldContentPrefab, uiGroup.transform);
            i--;
        }
    }
    void DestoryCloseField()
    {
        var childCount = uiGroup.transform.childCount;

        for (var idx = 0; idx < childCount; idx++)
        {
            var tran = uiGroup.transform.GetChild(idx);
            GameObject.Destroy(tran.gameObject);
        }
    }
}