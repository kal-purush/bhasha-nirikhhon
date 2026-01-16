using UnityEngine;
using System.Collections;

public class EnemyController : MonoBehaviour {

    public GameObject enemyLaser;
    public float speed = 5.0f;
    public float health = 150f;
    public bool isMovingRight;
    public float width = 3f;
    public int scoreValue = 150;
    public float shotsPerSecond = 0.5f;
    public float projectileSpeed = 16f;
    private float padding = 2f;
    private float xMin;
    private float xMax;
    private ScoreKeeper scoreKeeper;

	// Use this for initialization
	void Start () {
        float distance = transform.position.z - Camera.main.transform.position.z;
        Vector3 leftMost = Camera.main.ViewportToWorldPoint(new Vector3(0, 0, distance));
        Vector3 rightMost = Camera.main.ViewportToWorldPoint(new Vector3(1, 0, distance));
        xMin = leftMost.x + padding;
        xMax = rightMost.x - padding;
        transform.position += new Vector3(speed * Time.deltaTime, 0, 0);
        scoreKeeper = GameObject.Find("Score").GetComponent<ScoreKeeper>();
	}
	
	// Update is called once per frame
	void Update () {

        if (isMovingRight)
        {
            transform.position += Vector3.right * speed * Time.deltaTime;
        }
        else
        {
            transform.position += Vector3.left * speed * Time.deltaTime;
        }

        float rightEdgeOfFormation = transform.position.x + (0.5f * width);
        float leftEdgeOfFormation = transform.position.x - (0.5f * width);
        if (leftEdgeOfFormation < xMin)
        {
            isMovingRight = true;
        }
        else if (rightEdgeOfFormation > xMax)
        {
            isMovingRight = false;
        }

        // Fire random
        float probability = Time.deltaTime * shotsPerSecond;
        if (Random.value < probability)
        {
            Debug.Log("Enemy firing.");
            Fire();
        }
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
            Debug.Log("Enemy hit!");
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