using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BasicEnnemy : MonoBehaviour {

    public List<CombinationHandler.Button> Combination = new List<CombinationHandler.Button> { CombinationHandler.Button.BLUE };
    public int Life = 3;
    public float Speed = 0.1f;
    public bool IsMoving = true;
    public int NbrGold = 0;
    public Vector3 Position;
    public float SpawnCooldown = 1.0f;
    GameManager Gm;

    void Start()
    {
        Position = GetComponent<Transform>().position;
        Gm = GameManager.instance;
    }

    // Use this for initialization
    void Start (List<CombinationHandler.Button> combination, int nbrGold, Vector3 position) {
        Combination = combination;
        NbrGold = nbrGold;
        Position = position;
        Gm = GameManager.instance;
    }

    private void Attack()
    {
        Gm.NotifyDie(gameObject);
        Destroy(gameObject);
    }

    public void Die()
    {
        Gm.NotifyDie(gameObject);
        Destroy(gameObject);
    }

    private void Move()
    {
        Position.y -= Speed;
        GetComponent<Transform>().position = new Vector3(Position.x, Position.y, Position.z);
    }

    private void OnCollisionEnter(Collision collision)
    {
        if (collision.gameObject.name == "End")
            Attack();
    }

    // Update is called once per frame
    void Update () {
        Move();
	}
}