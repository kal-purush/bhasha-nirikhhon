using UnityEngine;

public class GameCanvas : MonoBehaviour
{

    Texture2D texture;
    public Vector2Int size;

    int index;

    // Use this for initialization
    void Start()
    {
        texture = new Texture2D(size.x, size.y);

        this.GetComponent<Renderer>().material.mainTexture = texture;

        Tick();

        


    }

    public  int[,] getPixels()
    {
        int [,] result = new int[];



    }


    public void Tick()
    {
        //do logic
        index++;


        UpdateCanvas();
    }


    void UpdateCanvas()
    {

        Color[] colors = new Color[size.x * size.y];

        for (int x = 0; x < size.x; x++)
        {
            for (int y = 0; y < size.y; y++)
            {
                if ((x * size.x) + y == index)
                    colors[(x * size.x) + y] = Color.red;
                else
                    colors[(x * size.x) + y] = Color.green;

            }
        }

        texture.SetPixels(colors);
        texture.Apply(true);
    }


    // Update is called once per frame
    void Update()
    {
        //Tick();
    }
}