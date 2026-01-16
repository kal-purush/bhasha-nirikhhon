using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Tile : MonoBehaviour
{

    SpriteRenderer sr;
    TileManager tm;
    public Sprite SpriteX, SpriteO;

    private void Start()
    {
        sr = GetComponent<SpriteRenderer>();
        tm = TileManager.tm;
    }
    private void OnMouseDown()
    {
        if (!sr.sprite && !tm.winBanner.GetComponent<SpriteRenderer>().sprite)
        {
            //Set an X or O when empty tile clicked
            if (tm.currentTurn == TileManager.TurnEnum.O)
            {
                sr.sprite = SpriteO;
                tm.currentTurn = TileManager.TurnEnum.X;
            }
            else
            {
                sr.sprite = SpriteX;
                tm.currentTurn = TileManager.TurnEnum.O;
            }

            //Check for winner
            bool won = false;
            //Check for bottom left to top right winner
            for (int i = 0; i < 3; i++)
            {
                if (tm.tiles[i, i].sr.sprite == tm.tiles[Mathf.Max(i - 1, 0), Mathf.Max(i - 1, 0)].sr.sprite && tm.tiles[i, i].sr.sprite)
                {
                    print("BL " + tm.tiles[i, i].sr.sprite.name + " " + i);
                    won = true;
                }
                else
                {
                    won = false;
                    break;
                }
            }
            if (won)
            {
                print("Bottom Left");
                tm.onWins();
            }
            //Check for top left to bottom right winner
            for (int i = 0; i < 3; i++)
            {
                int y = 2 - i;
                if (tm.tiles[i, y].sr.sprite == tm.tiles[Mathf.Max(i - 1, 0),Mathf.Min(y + 1,2)].sr.sprite && tm.tiles[i, y].sr.sprite)
                {
                    print("TL " + tm.tiles[i, y].sr.sprite.name + " " + i + " " + y);
                    won = true;
                }
                else
                {
                    won = false;
                    break;
                }
            }
            if (won)
            {
                print("Top Left");
                tm.onWins();
            }
            //Check for vertical winners
            for (int x = 0; x < 3; x++)
            {
                for (int y = 0; y < 3; y++)
                {
                    if (tm.tiles[x, y].sr.sprite == tm.tiles[x, Mathf.Max(y - 1, 0)].sr.sprite && tm.tiles[x, y].sr.sprite)
                    {
                        won = true;
                    }
                    else
                    {
                        won = false;
                        break;
                    }
                }
                if (won)
                {
                    print("Vertical");
                    tm.onWins();
                }
            }
            //Check for horizontal winners
            for (int y = 0; y < 3; y++)
            {
                for (int x = 0; x < 3; x++)
                {
                    if (tm.tiles[x, y].sr.sprite == tm.tiles[Mathf.Max(x - 1, 0), y].sr.sprite && tm.tiles[x, y].sr.sprite)
                    {
                        won = true;
                    }
                    else
                    {
                        won = false;
                        break;
                    }
                }
                if (won)
                {
                    print("Horizontal");
                    tm.onWins();
                }
            }
        }
    }
}