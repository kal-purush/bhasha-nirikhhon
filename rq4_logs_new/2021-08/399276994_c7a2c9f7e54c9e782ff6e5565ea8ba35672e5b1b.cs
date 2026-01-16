using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Board : MonoBehaviour
{
    public int width;
    public int height;    
   
    public GameObject[] elements;
    public GameObject[,] allElements;

    public Texture2D cursorPickAxe;
    public Texture2D cursorDefault;
    public bool cursorChanged;

    // Start is called before the first frame update
    void Start(){
        
        allElements = new GameObject[width, height];
        SetUp();
        //clickingTest();

        cursorChanged = false;
    }

    private void SetUp()
    {
        for(int i = 0; i < width; i++)
        {
            for(int j = 0; j < height; j++)
            {
                Vector2 tempPosition = new Vector2(i, j);
                int elementToUse = Random.Range(0, elements.Length);
                GameObject element = Instantiate(elements[elementToUse], tempPosition, Quaternion.identity);
                element.transform.parent = this.transform;
                element.name = "( " + i + ", " + j + ") ";
                allElements[i, j] = element;
            }
        }
        Camera.main.transform.position = new Vector3(4.6f, 4.4f, -10f);
        //canvas.gameObject.transform.position = new Vector3(3, 11, 100);
    }

    public void changeCursor()
    {
        if (cursorChanged)
        {
            Cursor.SetCursor(cursorPickAxe, Vector2.zero, CursorMode.Auto);
        }
        else if (!cursorChanged)
        {
            Cursor.SetCursor(cursorDefault, Vector2.zero, CursorMode.Auto);
        }
    }
        

    /*
    public void clickingTest()
    {
        //Get the mouse position on the screen and send a raycast into the game world from that position.
        Vector2 worldPoint = Camera.main.ScreenToWorldPoint(Input.mousePosition);
        RaycastHit2D hit = Physics2D.Raycast(worldPoint, Vector2.zero);

        //If something was hit, the RaycastHit2D.collider will not be null.
        if (hit.collider != null)
        {
            string colliderName = hit.collider.name;
            char x = colliderName[2];
            char y = colliderName[5];
            int xCoord = int.Parse(x.ToString());
            int yCoord = int.Parse(y.ToString());

            GameObject temp;
            temp = GameObject.Find(colliderName);
            Debug.Log(temp.name);
            temp.gameObject.GetComponent<Element>().enabled = true;
        }
    }
    */
}
 