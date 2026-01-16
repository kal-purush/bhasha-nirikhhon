using UnityEngine;

public class SpawningZombiesPoint : MonoBehaviour
{
    public Zombie zombiePrefab;
    public float waitTime = 1;
    private System.Random rnd;
    private int counter;

    public bool _spawiningNew = true;
    // Start is called before the first frame update
    void Awake()
    {
        counter = 0;
    }

    // Update is called once per frame
    void FixedUpdate()
    {
        int rnd = Random.Range(300,500);
        if (counter*waitTime > rnd) {
            Zombie zombie = Instantiate(this.zombiePrefab, this.transform.position, this.transform.rotation);
            counter = 0;
        } else {
            counter++;
        }
        //Debug.Log(rnd);
    }
}