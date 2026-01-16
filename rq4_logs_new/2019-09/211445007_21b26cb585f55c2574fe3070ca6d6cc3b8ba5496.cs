using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Slime : Entity
{
    public override bool findEnemy()
    {
        int directionIndex = 0;
        Tile currentSearch;
        for (int j = 0; j < 4; j++)
        {
            if (tileExists(directionIndex))
            {
                currentSearch = findTileDirection(curTile, j);
                if (currentSearch.tag != "Untagged")
                {
                    if (currentSearch.colliders.Count > 0)
                    {
                        for (int i = 0; i < currentSearch.colliders.Count; i++)
                        {
                            if (currentSearch.colliders[i].gameObject.GetComponent<Hero>())
                            {
                                target = currentSearch.colliders[i].gameObject.GetComponent<Hero>();
                                return true;
                            }
                        }
                    }
                }
            }
        }
        return false;

    }
    public override bool AttackTarget()
    {
        return target.Attacked(attack);
    }

}