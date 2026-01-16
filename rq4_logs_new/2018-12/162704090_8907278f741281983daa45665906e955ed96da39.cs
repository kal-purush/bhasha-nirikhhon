using System.Collections;
using UnityEngine;

public class GameWorld : MonoBehaviour
{

    public GameObject tilePrefab;
    public GameObject bodyPrefab;
    public Snake SnakePrefab;

    public Vector2Int size;

    public Snake snake;


    // Use this for initialization
    void Start()
    {
        GenerateWorld();
        snake = Snake.Instantiate<Snake>(SnakePrefab);
        snake.transform.SetParent(this.transform);

        snake.Body.Add(GameObject.Instantiate(bodyPrefab,snake.transform));
        snake.Body.Add(GameObject.Instantiate(bodyPrefab, snake.transform));
        snake.Body.Add(GameObject.Instantiate(bodyPrefab, snake.transform));
        snake.Body.Add(GameObject.Instantiate(bodyPrefab, snake.transform));

        var startPos = new Vector2Int( size.x/2, size.y/2);
        var startDir = Vector2Int.up;

        snake.Initialize(startPos, startDir);


        StartCoroutine("SlowUpdate");

    }

    void GenerateWorld()
    {
        for (int x = 0; x < size.x; x++)
        {
            for (int y = 0; y < size.y; y++)
            {
                var tile = GameObject.Instantiate(tilePrefab);
                tile.transform.SetParent(this.transform);
                tile.transform.eulerAngles = new Vector3(-90, 0, 0);
                tile.transform.localPosition = new Vector2(x, y);

            }
        }


    }


    public void Tick()
    {
        //snoepje

        snake.Tick();
    }

    public IEnumerator SlowUpdate()
    {
        while (true)
        {
            yield return new WaitForSeconds(1);
            Tick();


        }
    }

    // Update is called once per frame
    void Update()
    {
        if(Input.GetKey(KeyCode.UpArrow))
        {
            snake.dir = Vector3.up;
        }
        else if (Input.GetKey(KeyCode.DownArrow))
        {
            snake.dir = Vector3.down;
        }
        else if (Input.GetKey(KeyCode.LeftArrow))
        {
            snake.dir = Vector3.left;
        }
        else if(Input.GetKey(KeyCode.RightArrow))
        {
            snake.dir = Vector3.right;
        }
        //Tick();
    }
}