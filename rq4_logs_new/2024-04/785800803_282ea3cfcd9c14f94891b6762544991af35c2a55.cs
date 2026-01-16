using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MasterObject : MonoBehaviour
{
    // Variables
    public bool destruction = false;
    public GameObject[] Tetrominoes;
    public float previousTime;
    public float riseTime = 10f;
    public bool incrementing = false;
    public Vector3 newPosition;
    public GameObject Background;
    public Transform[,] grid = new Transform[100,200];
    void Start()
    {
        // Create a new tetromino
        NewTetromino();
        // Set the previous time to the current time
        previousTime = Time.time;
    }
    void Update()
    {
        // If the time is greater than the rise time and incrementing is false
        if( Time.time - previousTime > riseTime && incrementing == false){
            // Set incrementing to true
            incrementing = true;
            // Set the new position to the current position + 1
            newPosition = new Vector3(transform.position.x,transform.position.y + 1,transform.position.z);
        }
        // If incrementing is true
        if ( incrementing == true ){
            // Move the current position up
            transform.position += new Vector3 (0,.00002f,0); // Seems to cause missplacement of the tetromino
            // Move the background up
            Background.GetComponent<Transform>().position += new Vector3(0,.00002f,0);
            // If the current position is equal to the new position
            if (transform.position.y == newPosition.y){
                // Set incrementing to false
                incrementing = false;
                // Set the previous time to the current time
                previousTime = Time.time;
            }
        }
    }
    // Function to create a new tetromino
    public void NewTetromino(){
        // Create a new tetromino
        GameObject New = Instantiate(Tetrominoes[Random.Range(0,Tetrominoes.Length)], transform.position, Quaternion.identity);
        // Set the master to this
        New.GetComponent<TetrisBlock>().master = this;
    }
    // Destroy the grid values
    public void DestroyGrid(Transform t){
        Debug.Log("destroying grid values");
        // For each child in the transform
        foreach (Transform children in t){
            // Round the x and y values
            int roundedX = Mathf.RoundToInt(children.transform.position.x);
            int roundedY = Mathf.RoundToInt(children.transform.position.y);
            // If the grid value is not null
            if(grid[roundedX,roundedY] != null){
                // Destroy the game object
                Destroy(grid[roundedX,roundedY].gameObject);
                // Set the grid value to null
                grid[roundedX, roundedY] = null;
            }
        }
        // Destroy the transform
        Destroy(t.gameObject);
    }
    // Add the transform to the grid
    public void AddToGrid(Transform t){
        // If destruction is true
        if (destruction == true){
            // Destroy the grid
            DestroyGrid(t);
            return;
        }
        // For each child in the transform
        foreach (Transform children in t){
            // Round the x and y values
            int roundedX = Mathf.RoundToInt(children.transform.position.x);
            int roundedY = Mathf.RoundToInt(children.transform.position.y);
            // Add the child to the grid
            grid[roundedX, roundedY] = children;
        }
    }
    // Check the grid
    public bool checkGrid(int x, int y){
        // If destruction is true
        if (destruction == true){
            return false;
        }
        // If the grid value is not null
        if (grid[x,y] != null){
            return true;
        }
        // If the grid value is null
        return false;
    }
}