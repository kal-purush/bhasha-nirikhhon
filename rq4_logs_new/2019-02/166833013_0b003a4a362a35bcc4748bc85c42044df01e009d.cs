using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Preview : MonoBehaviour
{
    public GameObject wallPre1;
    public GameObject wallPre2;
    public GameObject wallPre3;

    private float blockSize, blockBuffer;
    public GridView gridView;

    private Transform thisTransform;

    void Awake()
    {
        wallPre1.SetActive(false);
        wallPre2.SetActive(false);
        wallPre3.SetActive(false);

        blockSize = gridView.blockSize;
        blockBuffer = gridView.blockBuffer;
        thisTransform = GetComponent<Transform>();
    }

    public void wallPreview(int column, int row, int cardtype)
    {
        bool isColumn = column % 2 == 1 ? true : false;
        bool isRow = row % 2 == 1 ? true : false;
        float xSize = 0, ySize = 0;

        if (isColumn)
        {
            xSize = (column + 1) * 0.5f * (blockSize * 7f + blockBuffer) - blockSize * 3f;
        }
        else
        {
            xSize = column * 0.5f * (blockSize * 7f + blockBuffer) + blockSize;
        }
        if (isRow)
        {
            ySize = (row + 1) * 0.5f * -(blockSize * 7f + blockBuffer) + blockSize * 3f;
        }
        else
        {
            ySize = row * 0.5f * -(blockSize * 7f + blockBuffer) - blockSize;
        }

        //thisTransform.position = new Vector3(xSize - 7.3f, ySize + 2.22f); // dot 위치로 이동

        switch (cardtype)
        {
            case 0:
                wallPre1.transform.position = new Vector3(xSize - 7.35f, ySize + 2.23f);
                wallPre1.SetActive(true);
                break;
            case 1:
                wallPre2.transform.position = new Vector3(xSize - 7.353f, ySize + 2.30f);
                wallPre2.SetActive(true);
                break;
            case 2:
                wallPre3.transform.position = new Vector3(xSize - 7.36f, ySize + 2.33f);
                wallPre3.SetActive(true);
                break;
        }
    }

    public void exitPreview()
    {
        wallPre1.SetActive(false);
        wallPre2.SetActive(false);
        wallPre3.SetActive(false);
    }
}