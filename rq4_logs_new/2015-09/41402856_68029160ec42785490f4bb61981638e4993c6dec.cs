using UnityEngine;
using System.Collections;

public class EnemyController : MonoBehaviour {

    public float speed = 30.0f;
    public float health = 150f;
    public int scoreValue = 150;
    public bool isMovingRight;
    [Header("Weapon Settings")]
    public GameObject enemyLaser;
    public float shotsPerSecond = 0.5f;
    public float projectileSpeed = 16f;
    [Header("Flight Pattern Settings")]
    public float pathSpeed = 50.0F;
    public Transform[] stage1_PathOneRight = new Transform[5];
    public Transform[] stage1_PathOneLeft = new Transform[5];
    public Transform[] entrance_BeginPathBottomLeft = new Transform[2];
    public Hashtable myTween = new Hashtable();
    public GameObject bottomLeftPt;
    private ScoreKeeper scoreKeeper;
    private EnemySpawner spawner;

	void Start () {
        
        spawner = GameObject.Find("EnemyFormation1").GetComponent<EnemySpawner>();
        GameObject spawnPt = GameObject.FindGameObjectWithTag("Respawn");
        GameObject middlePt = GameObject.FindGameObjectWithTag("Point2");
        GameObject rightEntrancePt = GameObject.FindGameObjectWithTag("begin_Right");
        bottomLeftPt = GameObject.FindGameObjectWithTag("Spawn_BottomLeft");

        //Left Entrance
        GameObject leftEntrancePt = GameObject.FindGameObjectWithTag("begin_Left");
        Transform nextSpawnPos = spawner.currentSpawnPos;

        // Test Bottom Left path
        entrance_BeginPathBottomLeft[0] = bottomLeftPt.transform;
        entrance_BeginPathBottomLeft[1] = leftEntrancePt.transform;

        //Testing itween
        if (nextSpawnPos)
        {
            // Entrance Left
            stage1_PathOneLeft[0] = spawnPt.transform;
            stage1_PathOneLeft[1] = middlePt.transform;
            stage1_PathOneLeft[2] = leftEntrancePt.transform;
            stage1_PathOneLeft[3] = spawner.gameObject.transform;
            stage1_PathOneLeft[4] = nextSpawnPos;
            // Entrance Right
            stage1_PathOneRight[0] = spawnPt.transform;
            stage1_PathOneRight[1] = middlePt.transform;
            stage1_PathOneRight[2] = rightEntrancePt.transform;
            stage1_PathOneRight[3] = spawner.gameObject.transform;
            stage1_PathOneRight[4] = nextSpawnPos;
        }

        //Alternate left to right entrance
        //Debug.Log("path goes:" + spawner.spawnEntranceRight);
        if (spawner.spawnEntranceRight == true)
        {
            myTween.Add("path", stage1_PathOneRight);
        }
        else
        {
            myTween.Add("path", stage1_PathOneLeft);
        }

        myTween.Add("speed", pathSpeed);
        scoreKeeper = GameObject.Find("Score").GetComponent<ScoreKeeper>();
        iTween.MoveTo(gameObject, myTween);
	}
	
    public void OnDrawGizmos()
    {
        iTween.DrawLine(stage1_PathOneRight);
        iTween.DrawLine(stage1_PathOneLeft, Color.blue);
    }

	// Update is called once per frame
	void Update () {
        // Fire random
        float probability = Time.deltaTime * shotsPerSecond;
        if (Random.value < probability)
        {
            //Debug.Log("Enemy firing.");
            //Fire();
        }
        //transform.Rotate(bottomLeftPt.transform.position, 90.0f);
        
        transform.rotation = Quaternion.AngleAxis(30, bottomLeftPt.transform.position);
	}

    void Fire()
    {
        Vector3 startPos = transform.position + new Vector3(0, 0, -4);
        GameObject enemyBullet = Instantiate(enemyLaser, startPos, Quaternion.identity) as GameObject;
        enemyBullet.GetComponent<Rigidbody>().velocity = new Vector3(0, 0, -projectileSpeed);
    }

    void OnTriggerEnter(Collider other)
    {
        Projectile playerBullet = other.gameObject.GetComponent<Projectile>();
        if (playerBullet)
        {
            health -= playerBullet.GetDamage();
            playerBullet.Hit();
            //Debug.Log("Enemy hit!");
            scoreKeeper.Score(scoreValue);
            if (health <= 0)
            {
                Destroy(gameObject);
                //end of game
                scoreKeeper.Score(200);
                Application.LoadLevel("Win Screen");
                //Die();
            }
        }
    }
}