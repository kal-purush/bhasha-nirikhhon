using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MasterObject : MonoBehaviour
{

    public bool destruction = false;
    public GameObject[] Tetrominoes;
    public Transform[,] grid = new Transform[10,20];
    // Start is called before the first frame update
    void Start()
    {
        NewTetromino();
    }

    // Update is called once per frame
    void Update()
    {

    }


    public void NewTetromino(){
        GameObject New = Instantiate(Tetrominoes[Random.Range(0,Tetrominoes.Length)], transform.position, Quaternion.identity);
        New.GetComponent<TetrisBlock>().master = this;
    }

    public void DestroyGrid(Transform t){
        Debug.Log("destroying grid values");
        foreach (Transform children in t){
            int roundedX = Mathf.RoundToInt(children.transform.position.x);
            int roundedY = Mathf.RoundToInt(children.transform.position.y);
            if(grid[roundedX,roundedY] != null){
                Destroy(grid[roundedX,roundedY].gameObject);
                grid[roundedX, roundedY] = null;
            }
        }
        Destroy(t.gameObject);
    }

    public void AddToGrid(Transform t){

        if (destruction == true){
            DestroyGrid(t);
            return;
        }
        foreach (Transform children in t){
            int roundedX = Mathf.RoundToInt(children.transform.position.x);
            int roundedY = Mathf.RoundToInt(children.transform.position.y);
            grid[roundedX, roundedY] = children;
        }
    }

    public bool checkGrid(int x, int y){
        if (destruction == true){
            return false;
        }
        if (grid[x,y] != null){
            return true;
        }
        return false;

    }
}