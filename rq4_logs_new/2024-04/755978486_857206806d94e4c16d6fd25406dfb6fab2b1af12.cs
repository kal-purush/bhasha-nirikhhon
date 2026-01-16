using UnityEngine;

public class LightsOut2D : MonoBehaviour
{

    public int gridSize = 5; // Adjust this value to change the grid size
    public LightsOut2DTile tilePrefab;
    public Color onColor;
    public Color offColor;
    public Color rewardColor;

    private LightsOut2DTile[] tiles;
    private bool[] lights;
    private bool rewardEarned;

    private void Start()
    {
        InitializeGrid();
    }

    private void InitializeGrid()
    {
        tiles = new LightsOut2DTile[gridSize];
        lights = new bool[gridSize];

        float startX = -(gridSize - 1) / 2f;
        float yOffset = 0.5f;

        for (int i = 0; i < gridSize; i++)
        {
            LightsOut2DTile tile = Instantiate(tilePrefab, new Vector3(startX + i, yOffset, 0), Quaternion.identity);
            tile.SetIndex(i);
            tiles[i] = tile;
            lights = RandomInitialLights(gridSize);

            SpriteRenderer renderer = tile.GetComponent<SpriteRenderer>();
            renderer.color = lights[i] ? onColor : offColor;
        }

        // The representative file should not be in game
        tilePrefab.gameObject.SetActive(false);
    }

    private void Update()
    {
        CheckWinCondition();
    }

    public void ToggleLights(int index)
    {
        // if already got reward, dont change anything
        if (rewardEarned)
        {
            return;
        }
        lights[index] = !lights[index];
        SpriteRenderer renderer = tiles[index].GetComponent<SpriteRenderer>();
        renderer.color = lights[index] ? onColor : offColor;

        if (index > 0)
        {
            lights[index - 1] = !lights[index - 1];
            SpriteRenderer prevRenderer = tiles[index - 1].GetComponent<SpriteRenderer>();
            prevRenderer.color = lights[index - 1] ? onColor : offColor;
        }

        if (index < gridSize - 1)
        {
            lights[index + 1] = !lights[index + 1];
            SpriteRenderer nextRenderer = tiles[index + 1].GetComponent<SpriteRenderer>();
            nextRenderer.color = lights[index + 1] ? onColor : offColor;

        }
    }

    private void CheckWinCondition()
    {
        bool allLightsOn = true;
        bool allLightsOff = true;

        foreach (bool light in lights)
        {
            if (light)
            {
                allLightsOff = false;
            }
            else
            {
                allLightsOn = false;
            }
        }

        if (allLightsOn)
        {
            rewardEarned = true;
            foreach (LightsOut2DTile tile in tiles)
            {
                SpriteRenderer renderer = tile.GetComponent<SpriteRenderer>();
                renderer.color = rewardColor;
            }
        }
    }

    private bool[] RandomInitialLights(int length)
    {
        // Initially, set all to true to make the puzzle solvable
        bool[] lights = new bool[length];

        for (int i = 0; i < length; i++)
        {
            lights[i] = true;
        }

        // Randomly toggle lights
        for (int i = 0; i < length; i++)
        {
            if (Random.value < 0.5f)
            {
                ToggleInitialLights(i, lights);
            }
        }

        return lights;
    }

    private void ToggleInitialLights(int index, bool[] lights)
    {
        lights[index] = !lights[index];
        if (index > 0)
        {
            lights[index - 1] = !lights[index - 1];
        }

        if (index < gridSize - 1)
        {
            lights[index + 1] = !lights[index + 1];
        }
    }
}

