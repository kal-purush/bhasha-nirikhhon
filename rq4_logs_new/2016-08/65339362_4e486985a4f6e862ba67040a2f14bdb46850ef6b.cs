using UnityEngine;
using System.Collections;

public class CollectObjects : MonoBehaviour
{

    public Player player1;
    public void OnTriggerEnter(Collider other)
    {
        Collectible collectible = other.GetComponent<Collectible>();

        if (collectible != null)
        {
            HUD.Instance.AdjustScore(collectible.score);
            if (player1 != null)
            {
                player1.m_Collect += collectible.score;
                //player1.CheckWinCondition();
            }
            if (collectible.type == Collectible.Type.Death)
            {
                Destroy(this.gameObject);
            }
            other.gameObject.SetActive(false);
        }
    }
}