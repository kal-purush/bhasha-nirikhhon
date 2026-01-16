using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BarLogic : MonoBehaviour
{
    public GameObject enemy;
    public GameObject green_bar;
    public GameObject red_bar;
    private EnemyHealthManager enemyHP;

    SpriteRenderer sr;
    // Start is called before the first frame update
    void Start()
    {
        sr = green_bar.GetComponent<SpriteRenderer>();
    }

    // Update is called once per frame
    void Update()
    {
        green_bar.transform.position = enemy.transform.position - new Vector3(0.3f - sr.bounds.size.x / 2, -0.4f);
        red_bar.transform.position = enemy.transform.position - new Vector3(0.17f, -0.4f);
        /*
        if(enemyHP.currentHealth <= 0)
        {
            Destroy(gameObject);
        }
        */
    }
    /*
    public void UpdateBar(float scale)
    {
        if (sr.transform.localScale.x > 0)
        {
            sr.transform.localScale -= new Vector3(scale, 0);
        }
    }*/
}