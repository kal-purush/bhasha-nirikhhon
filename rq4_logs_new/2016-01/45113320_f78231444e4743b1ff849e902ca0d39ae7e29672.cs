using UnityEngine;
using System.Collections;

public class SpawnerOption : MonoBehaviour {
    public float startDelaySpawn = 0.0f;
    public float waitBetweenSpawn = 1f;
    public int layerID = 1;
    public int numberToSpawn = 10;
    public bool canMove;
    public bool moveRight;
    public bool canHide;
    
    public int LifePoints=0 ;
    public int scoreValue = 0;

    public GameObject prefabEnnemy;
    Ennemy current;

    Transform trsm;
    bool randomScore, randomLife, alive;
    void Start () {
        trsm = this.gameObject.GetComponent<Transform>();
        if (LifePoints == 0)
        {
            randomLife = true;
        }
        else
        {
            randomLife = false;
        }
        if (scoreValue == 0)
        {
            randomScore = true;
        }
        else
        {
            randomScore = false;
        }
        StartCoroutine(Spawn());
    }
	
	// Update is called once per frame
	void Update () {
        if (alive)
        {
            CheckLife();
        }
	}

    IEnumerator Spawn()
    {
        if (startDelaySpawn > 0)
        {
            yield return new WaitForSeconds(startDelaySpawn);
            startDelaySpawn = 0;
        }
        else
        {
            yield return new WaitForSeconds(waitBetweenSpawn);
        }
        numberToSpawn--;
        Debug.Log("SPAWNING");
        current = ((GameObject) Instantiate(prefabEnnemy, trsm.position, prefabEnnemy.transform.rotation)).GetComponent<Ennemy>();
        current.gameObject.transform.parent = this.transform;
        Init();
    }

    void generateScore()
    {
        scoreValue = Mathf.FloorToInt(Random.Range(10, 50) * 10);
    }

    void generateLife()
    {
        LifePoints = Mathf.FloorToInt(Random.Range(1, 10));
    }

    void Init()
    {
        if (current != null)
        {
            current.canHide = canHide;
            current.canMove = canMove;
            if (canMove)
                current.gameObject.GetComponentInChildren<Move>().moveRight = moveRight;
            current.fight();
            //
            if (randomScore)
                generateScore();
            if (randomLife)
                generateLife();
            current.lifePoints = LifePoints;
            current.value = scoreValue;

            current.gameObject.GetComponentInChildren<SpriteRenderer>(). = layerID;
            alive = true;
            
        }
        
    }

    void CheckLife()
    {
        if (current != null)
        {
            if (current.lifePoints <= 0)
            {
                alive = false;
                Destroy(current.gameObject);

                if (numberToSpawn > 0)
                {
                    StartCoroutine(Spawn());
                }
            }
        }
        else
        {
            alive = false;
        }
        
    }
}